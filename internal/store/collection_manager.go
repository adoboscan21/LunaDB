/* ==========================================================
   Ruta y Archivo: ./internal/store/collection_manager.go
   ========================================================== */

package store

import (
	"log/slog"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"

	"lunadb/internal/globalconst"
)

// CollectionManager administra las colecciones (Buckets) activas en el disco.
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
	newCol.CreateIndex(globalconst.ID)

	cm.collections[name] = newCol
	slog.Info("Collection opened/created in disk engine", "name", name)
	return newCol
}

// DeleteCollection elimina una colección permanentemente del disco y de la memoria.
func (cm *CollectionManager) DeleteCollection(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	err := GlobalDB.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket([]byte(name))
	})

	if err != nil {
		if err == bbolt.ErrBucketNotFound {
			slog.Warn("Attempted to delete non-existent collection from disk", "name", name)
		} else {
			slog.Error("Failed to delete collection from disk", "name", name, "error", err)
		}
	} else {
		slog.Info("Collection deleted from disk", "name", name)
	}

	if _, exists := cm.collections[name]; exists {
		delete(cm.collections, name)
		slog.Info("Collection removed from memory manager", "name", name)
	}
}

// ListCollections lee directamente desde el disco los nombres de todos los Buckets activos.
func (cm *CollectionManager) ListCollections() []string {
	var names []string
	GlobalDB.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			names = append(names, string(name))
			return nil
		})
	})
	return names
}

// CollectionExists verifica directamente en el disco si un Bucket existe.
func (cm *CollectionManager) CollectionExists(name string) bool {
	exists := false
	GlobalDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b != nil {
			exists = true
		}
		return nil
	})
	return exists
}

// InitializeFromDisk es una función CRÍTICA para el arranque del servidor.
func (cm *CollectionManager) InitializeFromDisk() error {
	collectionNames := cm.ListCollections()
	slog.Info("Initializing collections from disk...", "count", len(collectionNames))

	for _, colName := range collectionNames {
		colStore := cm.GetCollection(colName)
		slog.Info("Rebuilding in-memory indexes for collection", "collection", colName)

		GlobalDB.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(colName))
			if b == nil {
				return nil
			}

			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var record ItemRecord
				if err := bson.Unmarshal(v, &record); err == nil {
					if record.TTL == 0 || time.Since(record.CreatedAt) <= record.TTL {
						indexedFields := colStore.ListIndexes()
						dataForIndex := extractIndexedValues(record.Value, indexedFields)

						if dataForIndex != nil {
							if ds, ok := colStore.(*DiskStore); ok {
								ds.indexes.Update(string(k), nil, dataForIndex)
							}
						}
					}
				}
			}
			return nil
		})
	}
	slog.Info("Finished initializing all collections and rebuilding indexes.")
	return nil
}

func (cm *CollectionManager) Wait()                                                {}
func (cm *CollectionManager) EnqueueSaveTask(collectionName string, col DataStore) {}
func (cm *CollectionManager) EnqueueDeleteTask(collectionName string)              {}

func (cm *CollectionManager) CleanExpiredItemsAndSave() {
	collectionNames := cm.ListCollections()
	slog.Info("Starting scheduled TTL sweep across all collections in disk")

	now := time.Now()
	for _, colName := range collectionNames {
		var keysToDelete [][]byte

		GlobalDB.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(colName))
			if b == nil {
				return nil
			}

			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var record ItemRecord
				if err := bson.Unmarshal(v, &record); err == nil {
					if record.TTL > 0 && now.After(record.CreatedAt.Add(record.TTL)) {
						keyCopy := make([]byte, len(k))
						copy(keyCopy, k)
						keysToDelete = append(keysToDelete, keyCopy)
					}
				}
			}
			return nil
		})

		if len(keysToDelete) > 0 {
			colStore := cm.GetCollection(colName)

			GlobalDB.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte(colName))
				if b == nil {
					return nil
				}
				for _, k := range keysToDelete {
					val := b.Get(k)
					if val != nil {
						var record ItemRecord
						if err := bson.Unmarshal(val, &record); err == nil {
							indexedFields := colStore.ListIndexes()
							oldDataForIndex := extractIndexedValues(record.Value, indexedFields)
							if ds, ok := colStore.(*DiskStore); ok && oldDataForIndex != nil {
								ds.indexes.Remove(string(k), oldDataForIndex)
							}
						}
					}
					b.Delete(k)
				}
				return nil
			})
			slog.Info("TTL cleaner removed expired items", "collection", colName, "count", len(keysToDelete))
		}
	}
}
