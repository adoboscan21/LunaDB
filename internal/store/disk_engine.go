package store

import (
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
)

var GlobalDB *bbolt.DB
var GlobalBatcher *WriteBatcher

// InitDiskEngine inicializa la base de datos bbolt y el Batcher.
func InitDiskEngine(dbPath string) error {
	slog.Info("Initializing Disk Engine (bbolt)...", "path", dbPath)

	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
		Timeout: 5 * time.Second,
		// 🔥 HEMOS QUITADO "NoSync: true".
		// Ahora bbolt hace 'fsync' físico por cada transacción. 100% seguro contra apagones.
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType, // Mantiene la solución al problema de RAM
	})
	if err != nil {
		return fmt.Errorf("failed to open bbolt database: %w", err)
	}

	GlobalDB = db

	// Inicializamos el Group Commit Worker
	GlobalBatcher = NewWriteBatcher()
	GlobalBatcher.Start()

	return nil
}

// CloseDiskEngine cierra la base de datos de forma segura.
func CloseDiskEngine() error {
	if GlobalBatcher != nil {
		close(GlobalBatcher.quit)
	}
	if GlobalDB != nil {
		slog.Info("Closing Disk Engine...")
		return GlobalDB.Close()
	}
	return nil
}

// =========================================================
// WRITE BATCHER (GROUP COMMIT)
// =========================================================

type BatchOp struct {
	Collection []byte
	Key        []byte
	Value      []byte
	IsDelete   bool
	Done       chan error // Canal para avisarle a la gorutina que ya se guardó seguro
}

type WriteBatcher struct {
	ops  chan *BatchOp
	quit chan struct{}
}

func NewWriteBatcher() *WriteBatcher {
	return &WriteBatcher{
		ops:  make(chan *BatchOp, 50000), // Buffer masivo para el Quincena Rush
		quit: make(chan struct{}),
	}
}

func (wb *WriteBatcher) Start() {
	go func() {
		var batch []*BatchOp
		// Espera máxima de 5 milisegundos para agrupar peticiones
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case op := <-wb.ops:
				batch = append(batch, op)
				// Si llegamos a 2000 operaciones juntas, escribimos de inmediato
				if len(batch) >= 2000 {
					wb.commitBatch(batch)
					batch = make([]*BatchOp, 0, 2000)
				}
			case <-ticker.C:
				if len(batch) > 0 {
					wb.commitBatch(batch)
					batch = make([]*BatchOp, 0, 2000)
				}
			case <-wb.quit:
				if len(batch) > 0 {
					wb.commitBatch(batch)
				}
				return
			}
		}
	}()
}

func (wb *WriteBatcher) commitBatch(batch []*BatchOp) {
	// UNA SOLA transacción ACID física en el disco duro para cientos de peticiones
	err := GlobalDB.Update(func(tx *bbolt.Tx) error {
		for _, op := range batch {
			b := tx.Bucket(op.Collection)
			if b != nil {
				if op.IsDelete {
					b.Delete(op.Key)
				} else {
					b.Put(op.Key, op.Value)
				}
			}
		}
		return nil
	})

	// Una vez seguro en el disco (fsync), despertamos a todas las gorutinas del cliente
	for _, op := range batch {
		op.Done <- err
	}
}

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
