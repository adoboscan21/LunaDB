package store

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"

	"go.etcd.io/bbolt"

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
	newCol.CreateIndex(globalconst.ID) // Registramos el índice principal en la metadata RAM

	cm.collections[name] = newCol
	slog.Info("Collection opened/created in sharded disk engine", "name", name)
	return newCol
}

// DeleteCollection elimina una colección permanentemente de TODOS los shards, incluyendo sus índices.
func (cm *CollectionManager) DeleteCollection(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Eliminamos el Bucket principal y sus Buckets de índices en todas las particiones
	for i := 0; i < TotalShards; i++ {
		err := GlobalDBs[i].Update(func(tx *bbolt.Tx) error {
			// Borramos la data principal
			tx.DeleteBucket([]byte(name))

			// Buscamos y borramos todos los Buckets de índices asociados a esta colección
			var bucketsToDelete [][]byte
			prefix := []byte("_idx_" + name + "_")

			tx.ForEach(func(bName []byte, _ *bbolt.Bucket) error {
				if bytes.HasPrefix(bName, prefix) {
					bucketsToDelete = append(bucketsToDelete, bName)
				}
				return nil
			})

			for _, bName := range bucketsToDelete {
				tx.DeleteBucket(bName)
			}
			return nil
		})

		if err != nil && err != bbolt.ErrBucketNotFound {
			slog.Error("Failed to delete collection from shard", "name", name, "shard_id", i, "error", err)
		}
	}
	slog.Info("Collection and its indexes deleted from all physical shards", "name", name)

	// Eliminamos de la memoria RAM
	if _, exists := cm.collections[name]; exists {
		delete(cm.collections, name)
		slog.Info("Collection removed from memory manager", "name", name)
	}
}

// ListCollections lee los nombres de todos los Buckets, filtrando los de índices internos.
func (cm *CollectionManager) ListCollections() []string {
	uniqueNames := make(map[string]struct{})

	for i := 0; i < TotalShards; i++ {
		GlobalDBs[i].View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
				nameStr := string(name)
				// Excluimos los buckets internos de índices para que no salgan en la lista de colecciones
				if !strings.HasPrefix(nameStr, "_idx_") {
					uniqueNames[nameStr] = struct{}{}
				}
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

// InitializeFromDisk es ultra rápido. Solo escanea la estructura del disco para reconstruir los metadatos.
func (cm *CollectionManager) InitializeFromDisk() error {
	slog.Info("Scanning disk to initialize index metadata...")

	// Recolectamos la información primero para evitar deadlocks de bbolt (View vs Update)
	foundIndexes := make(map[string][]string)

	for i := 0; i < TotalShards; i++ {
		GlobalDBs[i].View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
				nameStr := string(name)
				// Si detectamos un bucket de índice, registramos su metadata
				if strings.HasPrefix(nameStr, "_idx_") {
					// El formato es: _idx_NombreColeccion_NombreCampo
					parts := strings.SplitN(nameStr, "_", 4)
					if len(parts) >= 4 {
						colName := parts[2]
						fieldName := parts[3]
						foundIndexes[colName] = append(foundIndexes[colName], fieldName)
					}
				}
				return nil
			})
		})
	}

	// Aplicamos la metadata a los objetos en RAM de forma segura
	for colName, fields := range foundIndexes {
		col := cm.GetCollection(colName)
		if ds, ok := col.(*DiskStore); ok {
			for _, fieldName := range fields {
				ds.indexes.CreateIndex(fieldName)
			}
		}
	}

	slog.Info("Index metadata initialized successfully. Zero RAM overhead.")
	return nil
}

// Funciones de control de colas en desuso temporal (ahora manejadas por Group Commit Worker)
func (cm *CollectionManager) Wait()                                                {}
func (cm *CollectionManager) EnqueueSaveTask(collectionName string, col DataStore) {}
func (cm *CollectionManager) EnqueueDeleteTask(collectionName string)              {}

// ClearMemory purga el caché de colecciones en RAM sin tocar los discos físicos.
// Útil durante restauraciones masivas donde los discos cambian por debajo.
func (cm *CollectionManager) ClearMemory() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.collections = make(map[string]DataStore)
}
