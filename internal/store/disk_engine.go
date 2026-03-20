package store

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// EngineMetadata almacena la configuración estática inmutable del motor.
type EngineMetadata struct {
	NumShards int       `json:"num_shards"`
	CreatedAt time.Time `json:"created_at"`
}

// InitDiskEngine inicializa las múltiples bases de datos bbolt y sus Batchers correspondientes,
// garantizando que el número de particiones sea inmutable tras el primer arranque.
func InitDiskEngine(basePath string, requestedShards int) error {
	slog.Info("Initializing Disk Engine (Sharded bbolt)...", "basePath", basePath)

	if requestedShards <= 0 {
		requestedShards = 1 // Protección básica
	}

	// 1. GESTIÓN DE METADATOS INMUTABLES
	dirPath := filepath.Dir(basePath)
	metaPath := filepath.Join(dirPath, "system_metadata.json")

	metaBytes, err := os.ReadFile(metaPath)
	if err == nil {
		// El archivo existe, este es un arranque secundario.
		var meta EngineMetadata
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return fmt.Errorf("CRITICAL: Failed to parse system_metadata.json: %w", err)
		}

		if meta.NumShards != requestedShards {
			return fmt.Errorf(
				"CRITICAL FATAL ERROR: Configured NumShards (%d) does not match the immutable static value (%d) recorded in system_metadata.json. "+
					"Changing the number of shards on an existing database will cause irreparable data corruption. Boot aborted.",
				requestedShards, meta.NumShards,
			)
		}
		slog.Info("Verified immutable sharding configuration", "shards", meta.NumShards)
		TotalShards = meta.NumShards
	} else if os.IsNotExist(err) {
		// Primera vez que arranca. Escribimos la configuración en piedra.
		slog.Info("First boot detected. Creating immutable sharding configuration...", "shards", requestedShards)
		meta := EngineMetadata{
			NumShards: requestedShards,
			CreatedAt: time.Now().UTC(),
		}
		newMetaBytes, _ := json.MarshalIndent(meta, "", "  ")
		if writeErr := os.WriteFile(metaPath, newMetaBytes, 0644); writeErr != nil {
			return fmt.Errorf("CRITICAL: Failed to save system_metadata.json: %w", writeErr)
		}
		TotalShards = requestedShards
	} else {
		return fmt.Errorf("CRITICAL: Error accessing system_metadata.json: %w", err)
	}

	// 2. INICIALIZACIÓN DE LOS SHARDS (El resto de la función se mantiene igual)
	GlobalDBs = make([]*bbolt.DB, TotalShards)
	GlobalBatchers = make([]*WriteBatcher, TotalShards)

	// basePath normalmente es "data/luna.db". Lo separamos para añadir el sufijo de la partición.
	baseName := strings.TrimSuffix(basePath, ".db")

	for i := 0; i < TotalShards; i++ {
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

		// Inicializamos un Group Commit Worker EXCLUSIVO para este shard
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
				firstErr = err
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
		const maxBatch = 50000

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

// commitBatch ejecuta una sola transacción en disco para múltiples operaciones.
// GARANTÍA ACID: Ningún cliente recibe respuesta hasta que el disco confirme el fsync físico.
func (wb *WriteBatcher) commitBatch(batch []*BatchOp) {
	// Diccionario para almacenar errores lógicos/validación (ej. llave duplicada)
	// y evitar que un error individual aborte todo el batch masivo.
	validationErrors := make(map[*BatchOp]error)

	// Inicia la transacción bloqueante en bbolt
	err := wb.db.Update(func(tx *bbolt.Tx) error {
		// Agrupador masivo de escrituras validadas
		groupedWrites := make(map[string][]TxWrite)

		for _, op := range batch {
			if op.IsTx {
				// 1. VALIDACIÓN ULTRA RÁPIDA
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

				if txErr != nil {
					validationErrors[op] = txErr
					continue
				}

				// 2. Agrupamos sus escrituras lógicamente (NO avisamos al cliente aún)
				for _, write := range op.TxWrites {
					colStr := string(write.Collection)
					groupedWrites[colStr] = append(groupedWrites[colStr], write)
				}

			} else {
				// Lógica de Operación Simple
				_, err := tx.CreateBucketIfNotExists(op.Collection)
				if err != nil {
					validationErrors[op] = err
					continue
				}
				colStr := string(op.Collection)
				groupedWrites[colStr] = append(groupedWrites[colStr], TxWrite{
					Collection: op.Collection, Key: op.Key, Value: op.Value, IsDelete: op.IsDelete,
				})
			}
		}

		// 3. MUTACIÓN EN DISCO AGRUPADA POR BUCKET
		for bucketName, writes := range groupedWrites {
			b := tx.Bucket([]byte(bucketName))
			for _, write := range writes {
				if write.IsDelete {
					b.Delete(write.Key)
				} else {
					b.Put(write.Key, write.Value)
				}
			}
		}

		// bbolt hará el fsync FÍSICO al disco duro al retornar nil de esta función.
		return nil
	})

	// 4. RESOLUCIÓN DE DURABILIDAD (TRUE ACID)
	// Solo llegamos aquí si la luz NO se cortó y el disco duro confirmó la escritura.
	// Ahora sí, liberamos a todos los clientes bloqueados.
	for _, op := range batch {
		if valErr, hasErr := validationErrors[op]; hasErr {
			// El cliente falló por lógica (ej. clave duplicada)
			op.Done <- valErr
		} else {
			// El cliente recibe err (nil si el batch guardó exitosamente,
			// o un error catastrófico de disco duro si falló el I/O)
			op.Done <- err
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
