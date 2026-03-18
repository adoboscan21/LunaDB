package store

import (
	"bytes"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// DiskStore implementa DataStore usando bbolt particionado, con índices 100% en disco.
type DiskStore struct {
	collectionName []byte
	indexes        *IndexManager
}

// NewDiskStore inicializa el acceso a una colección asegurando que exista en todos los shards.
func NewDiskStore(name string) *DiskStore {
	colName := []byte(name)

	// Debemos asegurarnos de que el Bucket principal exista en TODAS las particiones físicas
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

// Set guarda o actualiza un documento y sus índices de forma atómica ruteándolo a su partición correspondiente.
func (s *DiskStore) Set(key string, value []byte) {
	shardID := GetShardID(key, TotalShards)
	batcher := GlobalBatchers[shardID]

	recordBytes, _ := bson.Marshal(ItemRecord{Value: value, CreatedAt: time.Now()})

	// 1. Escribir el documento principal
	writes := []TxWrite{
		{
			Collection: s.collectionName,
			Key:        []byte(key),
			Value:      recordBytes,
			IsDelete:   false,
		},
	}

	// 2. Extraer valores indexables y escribir en los Buckets de índices locales en el mismo Shard
	indexedFields := s.indexes.ListIndexes()
	newDataForIndex := extractIndexedValues(value, indexedFields)

	for field, val := range newDataForIndex {
		idxKey := encodeIndexKey(val, key)
		if idxKey != nil {
			idxBucket := []byte("_idx_" + string(s.collectionName) + "_" + field)
			writes = append(writes, TxWrite{
				Collection: idxBucket,
				Key:        idxKey,
				Value:      []byte{}, // Valor vacío, la llave tiene toda la información
				IsDelete:   false,
			})
		}
	}

	// 3. Enviar transacción al Batcher
	err := batcher.SubmitTx(writes)
	if err != nil {
		slog.Error("Failed to write document and indexes to disk batch", "key", key, "error", err)
	}
}

// SetMany guarda múltiples ítems y sus índices agrupándolos por partición.
func (s *DiskStore) SetMany(items map[string][]byte) {
	now := time.Now()
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

	// 2. Escribir en disco por cada Shard
	for shardID, sItems := range shardItems {
		db := GlobalDBs[shardID]
		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(s.collectionName)
			for key, value := range sItems {
				recordBytes, _ := bson.Marshal(ItemRecord{Value: value, CreatedAt: now})
				b.Put([]byte(key), recordBytes)

				newDataForIndex := extractIndexedValues(value, indexedFields)
				for field, val := range newDataForIndex {
					idxKey := encodeIndexKey(val, key)
					if idxKey != nil {
						idxB, _ := tx.CreateBucketIfNotExists([]byte("_idx_" + string(s.collectionName) + "_" + field))
						idxB.Put(idxKey, []byte{})
					}
				}
			}
			return nil
		})
	}
}

// Delete borra un registro y sus entradas de índice apuntando directamente a la partición correcta.
func (s *DiskStore) Delete(key string) {
	oldValue, found := s.Get(key)
	if !found {
		return
	}

	shardID := GetShardID(key, TotalShards)

	// Eliminar el documento principal
	writes := []TxWrite{
		{Collection: s.collectionName, Key: []byte(key), IsDelete: true},
	}

	// Eliminar los índices asociados
	indexedFields := s.indexes.ListIndexes()
	oldDataForIndex := extractIndexedValues(oldValue, indexedFields)

	for field, val := range oldDataForIndex {
		idxKey := encodeIndexKey(val, key)
		if idxKey != nil {
			idxBucket := []byte("_idx_" + string(s.collectionName) + "_" + field)
			writes = append(writes, TxWrite{Collection: idxBucket, Key: idxKey, IsDelete: true})
		}
	}

	err := GlobalBatchers[shardID].SubmitTx(writes)
	if err != nil {
		slog.Error("Failed to delete from disk batch", "key", key, "error", err)
	}
}

// DeleteMany borra múltiples ítems y sus índices agrupándolos por partición.
func (s *DiskStore) DeleteMany(keys []string) {
	indexedFields := s.indexes.ListIndexes()

	shardKeys := make(map[int][]string)
	for _, key := range keys {
		shardID := GetShardID(key, TotalShards)
		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

	for shardID, sKeys := range shardKeys {
		db := GlobalDBs[shardID]
		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(s.collectionName)
			if b == nil {
				return nil
			}
			for _, k := range sKeys {
				if oldVal := b.Get([]byte(k)); oldVal != nil {
					var record ItemRecord
					if bson.Unmarshal(oldVal, &record) == nil {
						oldData := extractIndexedValues(record.Value, indexedFields)
						for field, val := range oldData {
							idxKey := encodeIndexKey(val, k)
							if idxKey != nil {
								if idxB := tx.Bucket([]byte("_idx_" + string(s.collectionName) + "_" + field)); idxB != nil {
									idxB.Delete(idxKey)
								}
							}
						}
					}
				}
				b.Delete([]byte(k))
			}
			return nil
		})
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

	shardKeys := make(map[int][]string)
	for _, key := range keys {
		shardID := GetShardID(key, TotalShards)
		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

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

// StreamAll itera secuencialmente sobre todas las particiones (Full Table Scan).
func (s *DiskStore) StreamAll(cb func(key string, value []byte) bool) {
	var wg sync.WaitGroup
	var stop int32 // Bandera atómica para detener temprano si el cb devuelve false

	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				if atomic.LoadInt32(&stop) == 1 {
					return nil
				}
				b := tx.Bucket(s.collectionName)
				if b == nil {
					return nil
				}
				c := b.Cursor()

				for k, v := c.First(); k != nil; k, v = c.Next() {
					if atomic.LoadInt32(&stop) == 1 {
						break
					}
					raw := bson.Raw(v)
					if val := raw.Lookup("v"); val.Type == bsontype.Binary {
						_, data := val.Binary()
						if !cb(string(k), data) {
							atomic.StoreInt32(&stop, 1)
							break
						}
					}
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
}

// Update actualiza un ítem. Al usar Set, el motor rutea todo automáticamente.
func (s *DiskStore) Update(key string, newValue []byte) bool {
	s.Set(key, newValue)
	return true
}

// UpdateMany actualiza múltiples ítems e índices ruteando por partición.
func (s *DiskStore) UpdateMany(items map[string][]byte) (int, []string) {
	updatedCount := 0
	var failedKeys []string
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

				recordBytes, _ := bson.Marshal(ItemRecord{Value: newValue, CreatedAt: oldRecord.CreatedAt})

				if err := b.Put([]byte(key), recordBytes); err == nil {
					updatedCount++

					// Reemplazo de índices en disco
					oldData := extractIndexedValues(oldRecord.Value, indexedFields)
					newData := extractIndexedValues(newValue, indexedFields)

					for field := range oldData {
						if oldData[field] != newData[field] {
							if oldK := encodeIndexKey(oldData[field], key); oldK != nil {
								if idxB := tx.Bucket([]byte("_idx_" + string(s.collectionName) + "_" + field)); idxB != nil {
									idxB.Delete(oldK)
								}
							}
						}
					}
					for field, val := range newData {
						if oldData[field] != val {
							if newK := encodeIndexKey(val, key); newK != nil {
								idxB, _ := tx.CreateBucketIfNotExists([]byte("_idx_" + string(s.collectionName) + "_" + field))
								idxB.Put(newK, []byte{})
							}
						}
					}
				} else {
					failedKeys = append(failedKeys, key)
				}
			}
			return nil
		})
	}

	return updatedCount, failedKeys
}

// =========================================================
// MÉTODOS DE BÚSQUEDA POR ÍNDICE DIRECTO EN DISCO (B-TREE)
// =========================================================

// Lookup ejecuta la búsqueda en los índices B-Tree del disco de manera concurrente en todos los Shards.
func (s *DiskStore) Lookup(field string, value any) ([]string, bool) {
	if !s.HasIndex(field) {
		return nil, false
	}
	prefix := encodeIndexPrefix(value)
	if prefix == nil {
		return nil, true
	}

	var keys []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)

	// Disparamos la lectura en todos los discos al mismo tiempo
	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			var localKeys []string

			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(idxBucketName)
				if b == nil {
					return nil
				}
				c := b.Cursor()
				for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
					localKeys = append(localKeys, decodeDocID(k))
				}
				return nil
			})

			if len(localKeys) > 0 {
				mu.Lock()
				keys = append(keys, localKeys...)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	return keys, true
}

// LookupRange ejecuta la búsqueda por rangos de forma concurrente.
func (s *DiskStore) LookupRange(field string, low, high any, lInc, hInc bool) ([]string, bool) {
	if !s.HasIndex(field) {
		return nil, false
	}

	var keys []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	lowPrefix := encodeIndexPrefix(low)
	highPrefix := encodeIndexPrefix(high)
	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)

	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			var localKeys []string

			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(idxBucketName)
				if b == nil {
					return nil
				}
				c := b.Cursor()

				startKey := lowPrefix
				var k []byte
				if startKey != nil {
					k, _ = c.Seek(startKey)
				} else {
					k, _ = c.First()
				}

				for ; k != nil; k, _ = c.Next() {
					idx := bytes.LastIndexByte(k, 0x00)
					if idx == -1 {
						continue
					}
					valBytes := k[:idx+1]

					if highPrefix != nil {
						cmp := bytes.Compare(valBytes, highPrefix)
						if cmp > 0 {
							break
						}
						if !hInc && bytes.HasPrefix(k, highPrefix) {
							break
						}
					}

					if lowPrefix != nil && !lInc && bytes.HasPrefix(k, lowPrefix) {
						continue
					}

					localKeys = append(localKeys, decodeDocID(k))
				}
				return nil
			})

			if len(localKeys) > 0 {
				mu.Lock()
				keys = append(keys, localKeys...)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	return keys, true
}

func (s *DiskStore) StreamByIndex(field string, desc bool, cb func(key string) bool) bool {
	if !s.HasIndex(field) {
		return false
	}

	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)
	keepGoing := true

	for i := 0; i < TotalShards; i++ {
		GlobalDBs[i].View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(idxBucketName)
			if b == nil {
				return nil
			}
			c := b.Cursor()

			if desc {
				for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
					if !cb(decodeDocID(k)) {
						keepGoing = false
						break
					}
				}
			} else {
				for k, _ := c.First(); k != nil; k, _ = c.Next() {
					if !cb(decodeDocID(k)) {
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
	return true
}

// =========================================================
// MÉTODOS DE CONTROL DE ÍNDICES Y METADATOS
// =========================================================

func (s *DiskStore) CreateIndex(field string) {
	s.indexes.CreateIndex(field)
}

func (s *DiskStore) DeleteIndex(field string) {
	s.indexes.DeleteIndex(field)
	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)

	for i := 0; i < TotalShards; i++ {
		GlobalDBs[i].Update(func(tx *bbolt.Tx) error {
			tx.DeleteBucket(idxBucketName)
			return nil
		})
	}
}

func (s *DiskStore) ListIndexes() []string {
	return s.indexes.ListIndexes()
}

func (s *DiskStore) HasIndex(field string) bool {
	return s.indexes.HasIndex(field)
}

// Funciones de BI en desuso temporal tras mover los índices al disco duro
func (s *DiskStore) GetDistinctValues(field string) ([]any, bool) {
	if !s.HasIndex(field) {
		return nil, false
	}
	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)
	distinctMap := make(map[any]struct{})
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			localDistinct := make(map[any]struct{})
			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(idxBucketName)
				if b == nil {
					return nil
				}
				c := b.Cursor()
				var lastPrefix []byte

				for k, _ := c.First(); k != nil; k, _ = c.Next() {
					prefix := extractValueFromIndexKey(k)
					if lastPrefix != nil && bytes.Equal(prefix, lastPrefix) {
						continue // Salto hiper rápido para duplicados
					}
					lastPrefix = append([]byte(nil), prefix...)
					if val, ok := decodeIndexValue(prefix); ok {
						localDistinct[val] = struct{}{}
					}
				}
				return nil
			})
			if len(localDistinct) > 0 {
				mu.Lock()
				for v := range localDistinct {
					distinctMap[v] = struct{}{}
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	var results []any
	for v := range distinctMap {
		results = append(results, v)
	}
	return results, true
}

func (s *DiskStore) GetGroupedCount(field string) (map[any]int, bool) {
	if !s.HasIndex(field) {
		return nil, false
	}
	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)
	groupedMap := make(map[any]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			localCounts := make(map[any]int)
			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(idxBucketName)
				if b == nil {
					return nil
				}
				c := b.Cursor()
				var lastPrefix []byte
				var currentVal any
				var currentCount int

				for k, _ := c.First(); k != nil; k, _ = c.Next() {
					prefix := extractValueFromIndexKey(k)
					if lastPrefix == nil || !bytes.Equal(prefix, lastPrefix) {
						if lastPrefix != nil {
							localCounts[currentVal] += currentCount
						}
						lastPrefix = append([]byte(nil), prefix...)
						currentVal, _ = decodeIndexValue(prefix)
						currentCount = 1
					} else {
						currentCount++
					}
				}
				if lastPrefix != nil {
					localCounts[currentVal] += currentCount
				}
				return nil
			})
			if len(localCounts) > 0 {
				mu.Lock()
				for k, count := range localCounts {
					groupedMap[k] += count
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return groupedMap, true
}

// Interfaces stub
func (s *DiskStore) Size() int                       { return 0 }
func (s *DiskStore) LoadData(data map[string][]byte) {}
func (s *DiskStore) GetAll() map[string][]byte       { return nil }

// =========================================================
// MÉTODOS DE CARDINALIDAD (PARA EL QUERY PLANNER)
// =========================================================

// IndexCount cuenta cuántos documentos coinciden con un valor exacto.
func (s *DiskStore) IndexCount(field string, value any) int {
	if !s.HasIndex(field) {
		return 0
	}
	prefix := encodeIndexPrefix(value)
	if prefix == nil {
		return 0
	}

	count := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)

	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			localCount := 0
			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(idxBucketName)
				if b == nil {
					return nil
				}
				c := b.Cursor()
				for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
					localCount++
				}
				return nil
			})
			mu.Lock()
			count += localCount
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return count
}

// IndexRangeCount cuenta cuántos documentos caen dentro de un rango numérico o alfabético.
func (s *DiskStore) IndexRangeCount(field string, low, high any, lInc, hInc bool) int {
	if !s.HasIndex(field) {
		return 0
	}

	count := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	lowPrefix := encodeIndexPrefix(low)
	highPrefix := encodeIndexPrefix(high)
	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)

	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			localCount := 0

			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(idxBucketName)
				if b == nil {
					return nil
				}
				c := b.Cursor()

				startKey := lowPrefix
				var k []byte
				if startKey != nil {
					k, _ = c.Seek(startKey)
				} else {
					k, _ = c.First()
				}

				for ; k != nil; k, _ = c.Next() {
					idx := bytes.LastIndexByte(k, 0x00)
					if idx == -1 {
						continue
					}
					valBytes := k[:idx+1]

					if highPrefix != nil {
						cmp := bytes.Compare(valBytes, highPrefix)
						if cmp > 0 {
							break
						}
						if !hInc && bytes.HasPrefix(k, highPrefix) {
							break
						}
					}

					if lowPrefix != nil && !lInc && bytes.HasPrefix(k, lowPrefix) {
						continue
					}

					localCount++
				}
				return nil
			})

			mu.Lock()
			count += localCount
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return count
}
