package store

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

// GlobalDBs almacena las conexiones a cada una de las particiones (shards) físicas.
var GlobalDBs []*bbolt.DB

// GlobalBatchers almacena los workers de Group Commit, uno por cada partición.
var GlobalBatchers []*WriteBatcher

// TotalShards guarda la cantidad total de particiones configuradas para uso global.
var TotalShards int

// InitDiskEngine inicializa las múltiples bases de datos bbolt y sus Batchers correspondientes.
func InitDiskEngine(basePath string, numShards int) error {
	slog.Info("Initializing Disk Engine (Sharded bbolt)...", "basePath", basePath, "shards", numShards)

	if numShards <= 0 {
		numShards = 1 // Protección básica para asegurar al menos una partición
	}

	TotalShards = numShards
	GlobalDBs = make([]*bbolt.DB, numShards)
	GlobalBatchers = make([]*WriteBatcher, numShards)

	// basePath normalmente es "data/luna.db". Lo separamos para añadir el sufijo de la partición.
	baseName := strings.TrimSuffix(basePath, ".db")

	for i := 0; i < numShards; i++ {
		dbPath := fmt.Sprintf("%s_%d.db", baseName, i)
		db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
			Timeout: 5 * time.Second,
			// Mantenemos la seguridad extrema: bbolt hace 'fsync' físico por cada transacción.
			NoFreelistSync: true,
			FreelistType:   bbolt.FreelistMapType,
		})
		if err != nil {
			return fmt.Errorf("failed to open bbolt database shard %d: %w", i, err)
		}

		GlobalDBs[i] = db

		// Inicializamos un Group Commit Worker EXCLUSIVO para este shard, pasándole su BD específica
		batcher := NewWriteBatcher(db)
		GlobalBatchers[i] = batcher
		batcher.Start()
	}

	return nil
}

// CloseDiskEngine cierra todas las bases de datos de forma segura.
func CloseDiskEngine() error {
	slog.Info("Closing Sharded Disk Engine...")
	var firstErr error

	for i := 0; i < TotalShards; i++ {
		// 1. Apagamos el worker para que deje de aceptar operaciones
		if GlobalBatchers[i] != nil {
			close(GlobalBatchers[i].quit)
		}
		// 2. Cerramos la conexión al archivo físico
		if GlobalDBs[i] != nil {
			if err := GlobalDBs[i].Close(); err != nil && firstErr == nil {
				firstErr = err // Guardamos el primer error para reportarlo, pero seguimos cerrando el resto
			}
		}
	}
	return firstErr
}

// =========================================================
// WRITE BATCHER (GROUP COMMIT)
// =========================================================

// TxWrite define una operación atómica dentro de una transacción
type TxWrite struct {
	Collection   []byte
	Key          []byte
	Value        []byte
	IsDelete     bool
	MustExist    bool // Validación rápida Anti-Race Condition
	MustNotExist bool
}

type BatchOp struct {
	// Para operaciones simples (High-Speed Lane)
	Collection []byte
	Key        []byte
	Value      []byte
	IsDelete   bool

	// Para transacciones (Atomic Bundle)
	IsTx     bool
	TxWrites []TxWrite

	Done chan error // Canal para avisarle a la gorutina que ya se guardó seguro
}

type WriteBatcher struct {
	db   *bbolt.DB // Referencia directa a la base de datos de ESTE shard
	ops  chan *BatchOp
	quit chan struct{}
}

// NewWriteBatcher crea un nuevo agrupador de escrituras asignado a una base de datos específica.
func NewWriteBatcher(db *bbolt.DB) *WriteBatcher {
	return &WriteBatcher{
		db:   db,
		ops:  make(chan *BatchOp, 50000), // Buffer masivo para alta concurrencia
		quit: make(chan struct{}),
	}
}

// Start arranca la gorutina en segundo plano que agrupa y escribe en el disco.
// OPTIMIZADO: Usa 0% de CPU en reposo gracias a un Temporizador Perezoso (Lazy Timer).
func (wb *WriteBatcher) Start() {
	go func() {
		var batch []*BatchOp
		const maxBatch = 2000

		var timeoutCh <-chan time.Time
		var timer *time.Timer

		for {
			select {
			case op := <-wb.ops:
				batch = append(batch, op)

				// Si es el PRIMER elemento del lote, encendemos la cuenta regresiva de 5ms
				if len(batch) == 1 {
					timer = time.NewTimer(5 * time.Millisecond)
					timeoutCh = timer.C
				}

				// Si el lote se llenó a tope, escribimos ya mismo sin esperar los 5ms
				if len(batch) >= maxBatch {
					timer.Stop()    // Apagamos el reloj
					timeoutCh = nil // Desactivamos el canal del select

					wb.commitBatch(batch)
					batch = make([]*BatchOp, 0, maxBatch)
				}

			case <-timeoutCh:
				// Pasaron los 5ms y no se llenó el lote. Guardamos lo que haya.
				wb.commitBatch(batch)
				batch = make([]*BatchOp, 0, maxBatch)
				timeoutCh = nil // Volvemos a dormir profundamente (0% CPU)

			case <-wb.quit:
				if timer != nil {
					timer.Stop()
				}
				if len(batch) > 0 {
					wb.commitBatch(batch)
				}
				return
			}
		}
	}()
}

// commitBatch ejecuta una sola transacción en disco para múltiples operaciones en memoria.
func (wb *WriteBatcher) commitBatch(batch []*BatchOp) {
	// UNA SOLA transacción ACID física en el disco duro (específico de este Shard) para cientos de peticiones
	err := wb.db.Update(func(tx *bbolt.Tx) error {
		for _, op := range batch {
			if op.IsTx {
				// 1. VALIDACIÓN ULTRA RÁPIDA (Dentro del cerrojo de bbolt)
				var txErr error
				for _, write := range op.TxWrites {
					b, err := tx.CreateBucketIfNotExists(write.Collection)
					if err != nil {
						txErr = fmt.Errorf("failed to ensure bucket '%s': %w", write.Collection, err)
						break
					}

					exists := b.Get(write.Key) != nil

					if write.MustNotExist && exists {
						txErr = fmt.Errorf("transaction aborted: key conflict on '%s'", write.Key)
						break
					}
					if write.MustExist && !exists {
						txErr = fmt.Errorf("transaction aborted: key missing '%s'", write.Key)
						break
					}
				}

				// Si una validación falla, fallamos ESTA transacción, pero salvamos el resto del Batch
				if txErr != nil {
					op.Done <- txErr
					continue
				}

				// 2. MUTACIÓN (Garantizada y atómica)
				for _, write := range op.TxWrites {
					b := tx.Bucket(write.Collection)
					if write.IsDelete {
						b.Delete(write.Key)
					} else {
						b.Put(write.Key, write.Value)
					}
				}
				op.Done <- nil // Transacción Exitosa

			} else {
				// Lógica de Operación Simple (Single Write)
				b, err := tx.CreateBucketIfNotExists(op.Collection)
				if err == nil && b != nil {
					if op.IsDelete {
						b.Delete(op.Key)
					} else {
						b.Put(op.Key, op.Value)
					}
				}
				op.Done <- err
			}
		}
		return nil
	})

	// Si todo el bbolt Update falló por un error catastrófico de disco (muy raro), avisamos a todos
	if err != nil {
		for _, op := range batch {
			select {
			case op.Done <- err:
			default:
			}
		}
	}
}

// Submit manda una operación individual al batcher de esta partición.
func (wb *WriteBatcher) Submit(col, key, val []byte, isDel bool) error {
	done := make(chan error, 1)
	wb.ops <- &BatchOp{
		Collection: col,
		Key:        key,
		Value:      val,
		IsDelete:   isDel,
		Done:       done,
	}
	return <-done // Gorutina en pausa hasta que el disco duro diga OK
}

// SubmitTx manda un paquete de operaciones atómicas (Transacción) al batcher de esta partición.
func (wb *WriteBatcher) SubmitTx(ops []TxWrite) error {
	done := make(chan error, 1)
	wb.ops <- &BatchOp{
		IsTx:     true,
		TxWrites: ops,
		Done:     done,
	}
	return <-done
}
