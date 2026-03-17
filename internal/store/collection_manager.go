package store

import (
	"log/slog"
	"sync"

	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"

	"lunadb/internal/globalconst"
)

// CollectionManager administra las colecciones (Buckets) activas a lo largo de todos los shards.
type CollectionManager struct {
	collections map[string]DataStore
	mu          sync.RWMutex
}

// NewCollectionManager crea una nueva instancia del administrador de colecciones.
func NewCollectionManager() *CollectionManager {
	return &CollectionManager{
		collections: make(map[string]DataStore),
	}
}

// GetCollection obtiene una colección existente (DiskStore) por nombre, o la crea si no existe.
// Nota: NewDiskStore ya se encarga de crear el Bucket en todos los shards físicos.
func (cm *CollectionManager) GetCollection(name string) DataStore {
	cm.mu.RLock()
	col, found := cm.collections[name]
	cm.mu.RUnlock()
	if found {
		return col
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	col, found = cm.collections[name]
	if found {
		return col
	}

	newCol := NewDiskStore(name)
	newCol.CreateIndex(globalconst.ID) // Creamos el índice principal en RAM

	cm.collections[name] = newCol
	slog.Info("Collection opened/created in sharded disk engine", "name", name)
	return newCol
}

// DeleteCollection elimina una colección permanentemente de TODOS los shards y de la memoria.
func (cm *CollectionManager) DeleteCollection(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Eliminamos el Bucket físico en todas las particiones
	for i := 0; i < TotalShards; i++ {
		err := GlobalDBs[i].Update(func(tx *bbolt.Tx) error {
			return tx.DeleteBucket([]byte(name))
		})

		if err != nil {
			if err == bbolt.ErrBucketNotFound {
				// No es grave si en un shard específico no estaba el bucket, pero lo advertimos
				slog.Debug("Bucket not found during deletion on a specific shard", "name", name, "shard_id", i)
			} else {
				slog.Error("Failed to delete collection from shard", "name", name, "shard_id", i, "error", err)
			}
		}
	}
	slog.Info("Collection deleted from all physical shards", "name", name)

	// Eliminamos de la memoria RAM
	if _, exists := cm.collections[name]; exists {
		delete(cm.collections, name)
		slog.Info("Collection removed from memory manager", "name", name)
	}
}

// ListCollections hace una recolección (Scatter-Gather) para leer los nombres de todos los Buckets en todos los shards.
func (cm *CollectionManager) ListCollections() []string {
	uniqueNames := make(map[string]struct{})

	for i := 0; i < TotalShards; i++ {
		GlobalDBs[i].View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
				uniqueNames[string(name)] = struct{}{}
				return nil
			})
		})
	}

	var names []string
	for name := range uniqueNames {
		names = append(names, name)
	}
	return names
}

// CollectionExists verifica si un Bucket existe en al menos una partición del disco.
func (cm *CollectionManager) CollectionExists(name string) bool {
	exists := false

	for i := 0; i < TotalShards; i++ {
		GlobalDBs[i].View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(name))
			if b != nil {
				exists = true
			}
			return nil
		})
		if exists {
			break // Si ya lo encontramos en un shard, no hace falta buscar en los demás
		}
	}

	return exists
}

// InitializeFromDisk es una función CRÍTICA para el arranque del servidor.
// Reconstruye los índices en memoria RAM leyendo los datos distribuidos en todos los shards.
func (cm *CollectionManager) InitializeFromDisk() error {
	collectionNames := cm.ListCollections()
	slog.Info("Initializing collections from sharded disk...", "count", len(collectionNames), "total_shards", TotalShards)

	for _, colName := range collectionNames {
		colStore := cm.GetCollection(colName)
		slog.Info("Rebuilding in-memory indexes for collection", "collection", colName)

		// Debemos leer de cada fragmento (shard) para tener el índice completo en memoria
		for i := 0; i < TotalShards; i++ {
			GlobalDBs[i].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte(colName))
				if b == nil {
					return nil
				}

				c := b.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					var record ItemRecord
					if err := bson.Unmarshal(v, &record); err == nil {
						indexedFields := colStore.ListIndexes()
						dataForIndex := extractIndexedValues(record.Value, indexedFields)

						if dataForIndex != nil {
							if ds, ok := colStore.(*DiskStore); ok {
								ds.indexes.Update(string(k), nil, dataForIndex)
							}
						}
					}
				}
				return nil
			})
		}
	}
	slog.Info("Finished initializing all collections and rebuilding indexes.")
	return nil
}

// Funciones de control de colas (ahora no-ops para la persistencia directa y segura)
func (cm *CollectionManager) Wait()                                                {}
func (cm *CollectionManager) EnqueueSaveTask(collectionName string, col DataStore) {}
func (cm *CollectionManager) EnqueueDeleteTask(collectionName string)              {}
