package store

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"

	"lunadb/internal/globalconst"
)

type TransactionState int

const (
	StateActive TransactionState = iota
	StateCommitted
	StateAborted
)

type TransactionOpType int

const (
	OpTypeSet TransactionOpType = iota
	OpTypeUpdate
	OpTypeDelete
)

type WriteOperation struct {
	Collection string
	Key        string
	Value      []byte
	OpType     TransactionOpType
}

type Transaction struct {
	ID        string
	State     TransactionState
	WriteSet  []WriteOperation
	startTime time.Time
	mu        sync.RWMutex
}

type TransactionManager struct {
	transactions map[string]*Transaction
	mu           sync.RWMutex
	cm           *CollectionManager
	gcQuitChan   chan struct{}
	wg           sync.WaitGroup
}

// NewTransactionManager inicializa el gestor de transacciones en memoria.
func NewTransactionManager(cm *CollectionManager) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string]*Transaction),
		cm:           cm,
		gcQuitChan:   make(chan struct{}),
	}
}

// StartGC inicia el recolector de basura para transacciones abandonadas o huérfanas.
func (tm *TransactionManager) StartGC(timeout, interval time.Duration) {
	tm.wg.Add(1)
	go tm.runGC(timeout, interval)
}

// StopGC detiene el recolector de transacciones.
func (tm *TransactionManager) StopGC() {
	close(tm.gcQuitChan)
	tm.wg.Wait()
}

func (tm *TransactionManager) runGC(timeout, interval time.Duration) {
	defer tm.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var txIDsToRollback []string
			tm.mu.RLock()
			for txID, tx := range tm.transactions {
				tx.mu.RLock()
				if tx.State == StateActive && time.Since(tx.startTime) > timeout {
					txIDsToRollback = append(txIDsToRollback, txID)
				}
				tx.mu.RUnlock()
			}
			tm.mu.RUnlock()
			for _, txID := range txIDsToRollback {
				slog.Warn("Rolling back abandoned transaction", "txID", txID)
				tm.Rollback(txID)
			}
		case <-tm.gcQuitChan:
			return
		}
	}
}

// Begin inicia una nueva transacción.
func (tm *TransactionManager) Begin() (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txID := uuid.New().String()
	tm.transactions[txID] = &Transaction{
		ID:        txID,
		State:     StateActive,
		WriteSet:  make([]WriteOperation, 0),
		startTime: time.Now(),
	}
	return txID, nil
}

// RecordWrite encola una operación de escritura en la transacción actual.
func (tm *TransactionManager) RecordWrite(txID string, op WriteOperation) error {
	tm.mu.RLock()
	tx, exists := tm.transactions[txID]
	tm.mu.RUnlock()
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.State != StateActive {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	tx.WriteSet = append(tx.WriteSet, op)
	return nil
}

// Commit procesa la transacción enrutando las operaciones y sus índices a sus particiones.
func (tm *TransactionManager) Commit(txID string) error {
	tm.mu.RLock()
	tx, exists := tm.transactions[txID]
	tm.mu.RUnlock()
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	tx.mu.Lock()
	if tx.State != StateActive {
		tx.mu.Unlock()
		return fmt.Errorf("cannot commit, state is not active")
	}
	writeSetToProcess := tx.WriteSet
	tx.State = StateCommitted
	tx.mu.Unlock()

	now := time.Now().UTC()

	// 1. Agrupar las operaciones de escritura por Shard
	opsByShard := make(map[int][]WriteOperation)
	for _, op := range writeSetToProcess {
		shardID := GetShardID(op.Key, TotalShards)
		opsByShard[shardID] = append(opsByShard[shardID], op)
	}

	// Estructura para almacenar lo que mandaremos a cada Batcher
	txWritesByShard := make(map[int][]TxWrite)

	// --- FASE 1: PRE-PROCESAMIENTO OPTIMISTA Y ENSAMBLAJE DE ÍNDICES ---
	for shardID, ops := range opsByShard {
		db := GlobalDBs[shardID]
		var localTxWrites []TxWrite

		err := db.View(func(dbTx *bbolt.Tx) error {
			for _, op := range ops {
				var oldVal []byte
				b := dbTx.Bucket([]byte(op.Collection))
				if b != nil {
					if existingBytes := b.Get([]byte(op.Key)); existingBytes != nil {
						var rec ItemRecord
						if bson.Unmarshal(existingBytes, &rec) == nil {
							oldVal = rec.Value
						}
					}
				}

				// Validaciones lógicas iniciales
				if op.OpType == OpTypeSet && oldVal != nil {
					return fmt.Errorf("commit failed: key '%s' already exists in '%s'", op.Key, op.Collection)
				}
				if (op.OpType == OpTypeUpdate || op.OpType == OpTypeDelete) && oldVal == nil {
					return fmt.Errorf("commit failed: key '%s' does not exist in '%s'", op.Key, op.Collection)
				}

				if op.OpType != OpTypeDelete {
					// Parseo y ensamblaje de BSON en RAM
					var data bson.M
					bson.Unmarshal(op.Value, &data)
					data[globalconst.UPDATED_AT] = now
					if oldVal == nil {
						data[globalconst.CREATED_AT] = now
					}

					enrichedValue, _ := bson.Marshal(data)

					// === INTEGRACIÓN DE VERSIÓN (MVCC) Y LECTURA ESPERADA ===
					var expectedVersion uint64 = 0
					var newVersion uint64 = 1
					var checkVersion bool = false

					if oldVal != nil {
						b := dbTx.Bucket([]byte(op.Collection))
						if b != nil {
							if existingBytes := b.Get([]byte(op.Key)); existingBytes != nil {
								var tempRec ItemRecord
								if bson.Unmarshal(existingBytes, &tempRec) == nil {
									expectedVersion = tempRec.Version
									newVersion = tempRec.Version + 1
									checkVersion = true
								}
							}
						}
					}

					// Estructura persistente final
					newRec := ItemRecord{
						Value:     enrichedValue,
						CreatedAt: now,
						Version:   newVersion,
					}
					recBytes, _ := bson.Marshal(newRec)

					// Escribir documento principal
					localTxWrites = append(localTxWrites, TxWrite{
						Collection:      []byte(op.Collection),
						Key:             []byte(op.Key),
						Value:           recBytes,
						IsDelete:        false,
						MustExist:       op.OpType == OpTypeUpdate,
						MustNotExist:    op.OpType == OpTypeSet,
						ExpectedVersion: expectedVersion, // Registramos la versión
						CheckVersion:    checkVersion,    // Encendemos la verificación
					})

					// === GESTIÓN DE ÍNDICES ===
					colStore := tm.cm.GetCollection(op.Collection)
					if ds, ok := colStore.(*DiskStore); ok {
						indexedFields := ds.indexes.ListIndexes()

						// Si es un UPDATE, preparamos la eliminación de los índices viejos
						if oldVal != nil {
							oldData := extractIndexedValues(oldVal, indexedFields)
							for field, val := range oldData {
								if oldK := encodeIndexKey(val, op.Key); oldK != nil {
									idxBucket := []byte("_idx_" + op.Collection + "_" + field)
									localTxWrites = append(localTxWrites, TxWrite{
										Collection: idxBucket,
										Key:        oldK,
										IsDelete:   true,
									})
								}
							}
						}

						// Escribimos los índices nuevos (insert y update)
						newData := extractIndexedValues(enrichedValue, indexedFields)
						for field, val := range newData {
							if newK := encodeIndexKey(val, op.Key); newK != nil {
								idxBucket := []byte("_idx_" + op.Collection + "_" + field)
								localTxWrites = append(localTxWrites, TxWrite{
									Collection: idxBucket,
									Key:        newK,
									Value:      []byte{},
									IsDelete:   false,
								})
							}
						}
					}

				} else {
					// OpTypeDelete: Recuperar la versión esperada para el borrado seguro
					var expectedVersion uint64 = 0
					var checkVersion bool = false

					if oldVal != nil {
						b := dbTx.Bucket([]byte(op.Collection))
						if b != nil {
							if existingBytes := b.Get([]byte(op.Key)); existingBytes != nil {
								var tempRec ItemRecord
								if bson.Unmarshal(existingBytes, &tempRec) == nil {
									expectedVersion = tempRec.Version
									checkVersion = true
								}
							}
						}
					}

					localTxWrites = append(localTxWrites, TxWrite{
						Collection:      []byte(op.Collection),
						Key:             []byte(op.Key),
						IsDelete:        true,
						MustExist:       true,
						ExpectedVersion: expectedVersion, // Registramos la versión
						CheckVersion:    checkVersion,    // Encendemos la verificación
					})

					// === BORRADO DE ÍNDICES ===
					colStore := tm.cm.GetCollection(op.Collection)
					if ds, ok := colStore.(*DiskStore); ok && oldVal != nil {
						indexedFields := ds.indexes.ListIndexes()
						oldData := extractIndexedValues(oldVal, indexedFields)
						for field, val := range oldData {
							if oldK := encodeIndexKey(val, op.Key); oldK != nil {
								idxBucket := []byte("_idx_" + op.Collection + "_" + field)
								localTxWrites = append(localTxWrites, TxWrite{
									Collection: idxBucket,
									Key:        oldK,
									IsDelete:   true,
								})
							}
						}
					}
				}
			}
			return nil
		})

		if err != nil {
			tm.Rollback(txID)
			return err // Fallo en la fase optimista, abortamos todo.
		}

		txWritesByShard[shardID] = localTxWrites
	}

	// --- FASE 2: DELEGACIÓN PARALELA A LOS WRITE BATCHERS ---
	type channelResult struct {
		shardID int
		err     error
	}

	// Mandamos las operaciones a cada Shard simultáneamente
	errCh := make(chan channelResult, len(txWritesByShard))

	for shardID, txWrites := range txWritesByShard {
		go func(sID int, writes []TxWrite) {
			errCh <- channelResult{shardID: sID, err: GlobalBatchers[sID].SubmitTx(writes)}
		}(shardID, txWrites)
	}

	// Esperamos a que todos los shards confirmen la escritura atómica en el disco duro
	var commitErr error
	for i := 0; i < len(txWritesByShard); i++ {
		res := <-errCh
		if res.err != nil && commitErr == nil {
			commitErr = res.err
		}
	}

	if commitErr != nil {
		tm.Rollback(txID)
		return commitErr
	}

	// Limpieza de la memoria del TransactionManager
	tm.mu.Lock()
	delete(tm.transactions, txID)
	tm.mu.Unlock()

	slog.Info("Transaction committed with indexes to sharded disk via Parallel Group Commit", "txID", txID, "shards_touched", len(txWritesByShard))
	return nil
}

// Rollback cancela y limpia la transacción de la memoria.
func (tm *TransactionManager) Rollback(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tx, exists := tm.transactions[txID]; exists {
		tx.mu.Lock()
		tx.State = StateAborted
		tx.mu.Unlock()
		delete(tm.transactions, txID)
	}
	return nil
}
