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

// Commit procesa la transacción enrutando las operaciones a sus respectivas particiones (Shards).
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

	// Estructura temporal para recordar qué actualizar en los índices RAM después
	type meta struct {
		op            WriteOperation
		oldValue      []byte
		enrichedValue []byte
	}
	metaList := make([]meta, 0, len(writeSetToProcess))

	// --- FASE 1: PRE-PROCESAMIENTO OPTIMISTA (Por cada Shard involucrado) ---
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

				m := meta{op: op, oldValue: oldVal}

				// Parseo y ensamblaje de BSON en RAM
				if op.OpType != OpTypeDelete {
					var data bson.M
					bson.Unmarshal(op.Value, &data)
					data[globalconst.UPDATED_AT] = now
					if oldVal == nil {
						data[globalconst.CREATED_AT] = now
					}

					enrichedValue, _ := bson.Marshal(data)
					m.enrichedValue = enrichedValue

					// Estructura persistente final
					rec := ItemRecord{Value: enrichedValue, CreatedAt: now}
					recBytes, _ := bson.Marshal(rec)

					localTxWrites = append(localTxWrites, TxWrite{
						Collection:   []byte(op.Collection),
						Key:          []byte(op.Key),
						Value:        recBytes,
						IsDelete:     false,
						MustExist:    op.OpType == OpTypeUpdate,
						MustNotExist: op.OpType == OpTypeSet,
					})
				} else {
					localTxWrites = append(localTxWrites, TxWrite{
						Collection: []byte(op.Collection),
						Key:        []byte(op.Key),
						IsDelete:   true,
						MustExist:  true,
					})
				}
				metaList = append(metaList, m)
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
	// Mandamos las operaciones a cada Shard simultáneamente
	errCh := make(chan error, len(txWritesByShard))
	for shardID, txWrites := range txWritesByShard {
		go func(sID int, writes []TxWrite) {
			errCh <- GlobalBatchers[sID].SubmitTx(writes)
		}(shardID, txWrites)
	}

	// Esperamos a que todos los shards confirmen la escritura
	var commitErr error
	for i := 0; i < len(txWritesByShard); i++ {
		if err := <-errCh; err != nil && commitErr == nil {
			commitErr = err
		}
	}

	if commitErr != nil {
		// Si un batcher falla, reportamos el error.
		// (Nota: Sin un log WAL distribuido, podría haber un commit parcial si un shard triunfó y otro falló por disco dañado).
		tm.Rollback(txID)
		return commitErr
	}

	// --- FASE 3: ACTUALIZAR ÍNDICES EN RAM POST-COMMIT ---
	for _, m := range metaList {
		col := tm.cm.GetCollection(m.op.Collection)
		if ds, ok := col.(*DiskStore); ok {
			indexedFields := ds.indexes.ListIndexes()
			var oldDataForIndex, newDataForIndex map[string]any

			if m.oldValue != nil {
				oldDataForIndex = extractIndexedValues(m.oldValue, indexedFields)
			}
			if m.op.OpType != OpTypeDelete {
				newDataForIndex = extractIndexedValues(m.enrichedValue, indexedFields)
			}

			ds.indexes.Update(m.op.Key, oldDataForIndex, newDataForIndex)
		}
	}

	// Limpieza de la memoria del TransactionManager
	tm.mu.Lock()
	delete(tm.transactions, txID)
	tm.mu.Unlock()

	slog.Info("Transaction successfully committed to sharded disk via Parallel Group Commit", "txID", txID, "ops_count", len(metaList), "shards_touched", len(txWritesByShard))
	return nil
}

// Rollback cancela y limpia la transacción.
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
