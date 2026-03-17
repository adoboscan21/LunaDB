package store

import (
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// DiskStore implementa DataStore usando bbolt particionado.
type DiskStore struct {
	collectionName []byte
	indexes        *IndexManager
}

// NewDiskStore inicializa el acceso a una colección asegurando que exista en todos los shards.
func NewDiskStore(name string) *DiskStore {
	colName := []byte(name)

	// Debemos asegurarnos de que el Bucket exista en TODAS las particiones físicas
	for i := 0; i < TotalShards; i++ {
		GlobalDBs[i].Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(colName)
			return err
		})
	}

	return &DiskStore{
		collectionName: colName,
		indexes:        NewIndexManager(),
	}
}

// Set guarda o actualiza un documento ruteándolo a su partición (shard) correspondiente.
func (s *DiskStore) Set(key string, value []byte) {
	shardID := GetShardID(key, TotalShards)
	batcher := GlobalBatchers[shardID]

	record := ItemRecord{
		Value:     value,
		CreatedAt: time.Now(),
	}

	recordBytes, err := bson.Marshal(record)
	if err != nil {
		slog.Error("Failed to marshal ItemRecord", "key", key, "error", err)
		return
	}

	// Enviamos al Batcher específico de este shard
	err = batcher.Submit(s.collectionName, []byte(key), recordBytes, false)
	if err != nil {
		slog.Error("Failed to write to disk batch", "key", key, "error", err)
		return
	}

	// Actualizamos el índice en RAM
	indexedFields := s.indexes.ListIndexes()
	newDataForIndex := extractIndexedValues(value, indexedFields)
	if newDataForIndex != nil {
		s.indexes.Update(key, nil, newDataForIndex)
	}
}

// SetMany guarda múltiples ítems agrupándolos por partición.
func (s *DiskStore) SetMany(items map[string][]byte) {
	now := time.Now()
	indexUpdates := make(map[string]map[string]any, len(items))
	indexedFields := s.indexes.ListIndexes()

	// 1. Agrupar ítems por Shard
	shardItems := make(map[int]map[string][]byte)
	for key, value := range items {
		shardID := GetShardID(key, TotalShards)
		if shardItems[shardID] == nil {
			shardItems[shardID] = make(map[string][]byte)
		}
		shardItems[shardID][key] = value
	}

	// 2. Escribir en disco por cada Shard de manera atómica local
	for shardID, sItems := range shardItems {
		db := GlobalDBs[shardID]
		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(s.collectionName)
			for key, value := range sItems {
				record := ItemRecord{Value: value, CreatedAt: now}
				recordBytes, _ := bson.Marshal(record)
				b.Put([]byte(key), recordBytes)

				if newDataForIndex := extractIndexedValues(value, indexedFields); newDataForIndex != nil {
					indexUpdates[key] = newDataForIndex
				}
			}
			return nil
		})
	}

	// 3. Actualización de índices en RAM
	for key, data := range indexUpdates {
		s.indexes.Update(key, nil, data)
	}
}

// Delete borra un registro apuntando directamente a la partición correcta.
func (s *DiskStore) Delete(key string) {
	oldValue, found := s.Get(key)

	shardID := GetShardID(key, TotalShards)
	batcher := GlobalBatchers[shardID]

	// Eliminación ruteada
	err := batcher.Submit(s.collectionName, []byte(key), nil, true)
	if err != nil {
		slog.Error("Failed to delete from disk batch", "key", key, "error", err)
		return
	}

	if found {
		indexedFields := s.indexes.ListIndexes()
		oldDataForIndex := extractIndexedValues(oldValue, indexedFields)
		if oldDataForIndex != nil {
			s.indexes.Remove(key, oldDataForIndex)
		}
	}
}

// DeleteMany borra múltiples ítems agrupándolos por partición.
func (s *DiskStore) DeleteMany(keys []string) {
	oldDatas := make(map[string]map[string]any, len(keys))
	indexedFields := s.indexes.ListIndexes()

	// 1. Agrupar llaves por Shard
	shardKeys := make(map[int][]string)
	for _, key := range keys {
		shardID := GetShardID(key, TotalShards)
		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

	// 2. Ejecutar borrado partición por partición
	for shardID, sKeys := range shardKeys {
		db := GlobalDBs[shardID]
		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(s.collectionName)
			if b == nil {
				return nil
			}
			for _, k := range sKeys {
				kb := []byte(k)
				if oldVal := b.Get(kb); oldVal != nil {
					var record ItemRecord
					if err := bson.Unmarshal(oldVal, &record); err == nil {
						if oldDataForIndex := extractIndexedValues(record.Value, indexedFields); oldDataForIndex != nil {
							oldDatas[k] = oldDataForIndex
						}
					}
				}
				b.Delete(kb)
			}
			return nil
		})
	}

	for k, oldData := range oldDatas {
		s.indexes.Remove(k, oldData)
	}
}

// Get lee el documento buscando directamente en el Shard correcto en O(1).
func (s *DiskStore) Get(key string) ([]byte, bool) {
	shardID := GetShardID(key, TotalShards)
	db := GlobalDBs[shardID]

	var valCopy []byte
	var found bool

	db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		if b == nil {
			return nil
		}

		recordBytes := b.Get([]byte(key))
		if recordBytes != nil {
			var record ItemRecord
			if err := bson.Unmarshal(recordBytes, &record); err == nil {
				valCopy = make([]byte, len(record.Value))
				copy(valCopy, record.Value)
				found = true
			}
		}
		return nil
	})

	return valCopy, found
}

// GetMany recupera múltiples ítems agrupando las consultas por partición.
func (s *DiskStore) GetMany(keys []string) map[string][]byte {
	res := make(map[string][]byte, len(keys))

	// Agrupar llaves por Shard
	shardKeys := make(map[int][]string)
	for _, key := range keys {
		shardID := GetShardID(key, TotalShards)
		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

	// Consultar partición por partición
	for shardID, sKeys := range shardKeys {
		db := GlobalDBs[shardID]
		db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(s.collectionName)
			if b == nil {
				return nil
			}
			for _, k := range sKeys {
				if recordBytes := b.Get([]byte(k)); recordBytes != nil {
					raw := bson.Raw(recordBytes)
					if val := raw.Lookup("v"); val.Type == bsontype.Binary {
						_, data := val.Binary()
						valCopy := make([]byte, len(data))
						copy(valCopy, data)
						res[k] = valCopy
					}
				}
			}
			return nil
		})
	}

	return res
}

// StreamAll itera secuencialmente sobre todas las particiones.
func (s *DiskStore) StreamAll(callback func(key string, value []byte) bool) {
	keepGoing := true
	for i := 0; i < TotalShards; i++ {
		db := GlobalDBs[i]
		db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(s.collectionName)
			if b == nil {
				return nil
			}
			c := b.Cursor()

			for k, v := c.First(); k != nil; k, v = c.Next() {
				raw := bson.Raw(v)
				if val := raw.Lookup("v"); val.Type == bsontype.Binary {
					_, data := val.Binary()
					if !callback(string(k), data) {
						keepGoing = false
						break
					}
				}
			}
			return nil
		})
		if !keepGoing {
			break
		}
	}
}

// Update actualiza un ítem. Al usar Set, ya rutea automáticamente.
func (s *DiskStore) Update(key string, newValue []byte) bool {
	s.Set(key, newValue)
	return true
}

// UpdateMany actualiza múltiples ítems ruteando las operaciones por partición.
func (s *DiskStore) UpdateMany(items map[string][]byte) (int, []string) {
	updatedCount := 0
	var failedKeys []string
	indexUpdates := make(map[string]map[string]any, len(items))
	indexedFields := s.indexes.ListIndexes()

	shardItems := make(map[int]map[string][]byte)
	for key, value := range items {
		shardID := GetShardID(key, TotalShards)
		if shardItems[shardID] == nil {
			shardItems[shardID] = make(map[string][]byte)
		}
		shardItems[shardID][key] = value
	}

	for shardID, sItems := range shardItems {
		db := GlobalDBs[shardID]
		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(s.collectionName)
			for key, newValue := range sItems {
				existing := b.Get([]byte(key))
				if existing == nil {
					failedKeys = append(failedKeys, key)
					continue
				}

				var oldRecord ItemRecord
				bson.Unmarshal(existing, &oldRecord)

				record := ItemRecord{
					Value:     newValue,
					CreatedAt: oldRecord.CreatedAt,
				}
				recordBytes, _ := bson.Marshal(record)

				if err := b.Put([]byte(key), recordBytes); err == nil {
					updatedCount++
					if newDataForIndex := extractIndexedValues(newValue, indexedFields); newDataForIndex != nil {
						indexUpdates[key] = newDataForIndex
					}
				} else {
					failedKeys = append(failedKeys, key)
				}
			}
			return nil
		})
	}

	for key, data := range indexUpdates {
		s.indexes.Update(key, nil, data)
	}

	return updatedCount, failedKeys
}

// === Funciones de Índices (Operan en memoria globalmente) ===
func (s *DiskStore) CreateIndex(field string)   { s.indexes.CreateIndex(field) }
func (s *DiskStore) DeleteIndex(field string)   { s.indexes.DeleteIndex(field) }
func (s *DiskStore) ListIndexes() []string      { return s.indexes.ListIndexes() }
func (s *DiskStore) HasIndex(field string) bool { return s.indexes.HasIndex(field) }
func (s *DiskStore) Lookup(field string, value any) ([]string, bool) {
	return s.indexes.Lookup(field, value)
}
func (s *DiskStore) LookupRange(field string, low, high any, lInc, hInc bool) ([]string, bool) {
	return s.indexes.LookupRange(field, low, high, lInc, hInc)
}
func (s *DiskStore) StreamByIndex(field string, desc bool, cb func(key string) bool) bool {
	return s.indexes.StreamByIndex(field, desc, cb)
}
func (s *DiskStore) GetDistinctValues(field string) ([]any, bool) {
	return s.indexes.GetDistinctValues(field)
}
func (s *DiskStore) GetGroupedCount(field string) (map[any]int, bool) {
	return s.indexes.GetGroupedCount(field)
}

func (s *DiskStore) Size() int                       { return 0 }
func (s *DiskStore) LoadData(data map[string][]byte) {}
func (s *DiskStore) GetAll() map[string][]byte       { return nil }
