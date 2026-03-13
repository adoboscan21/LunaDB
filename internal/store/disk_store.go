/* ==========================================================
   Ruta y Archivo: ./internal/store/disk_store.go
   ========================================================== */

package store

import (
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// DiskStore implementa DataStore usando bbolt.
type DiskStore struct {
	collectionName []byte
	indexes        *IndexManager
}

// NewDiskStore inicializa el acceso a una colección en disco.
func NewDiskStore(name string) *DiskStore {
	colName := []byte(name)

	GlobalDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(colName)
		return err
	})

	return &DiskStore{
		collectionName: colName,
		indexes:        NewIndexManager(),
	}
}

// Set serializa el valor y lo guarda en disco usando Update directo (Latencia ultra baja)
func (s *DiskStore) Set(key string, value []byte, ttl time.Duration) {
	record := ItemRecord{
		Value:     value,
		CreatedAt: time.Now(),
		TTL:       ttl,
	}

	recordBytes, err := bson.Marshal(record)
	if err != nil {
		slog.Error("Failed to marshal ItemRecord", "key", key, "error", err)
		return
	}

	// OPTIMIZADO: Cambiado de Batch a Update para evitar el retraso de agrupación de bbolt
	GlobalDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		return b.Put([]byte(key), recordBytes)
	})

	indexedFields := s.indexes.ListIndexes()
	newDataForIndex := extractIndexedValues(value, indexedFields)
	if newDataForIndex != nil {
		s.indexes.Update(key, nil, newDataForIndex)
	}
}

// SetMany guarda múltiples ítems en una sola transacción ACID (Bulk Insert)
func (s *DiskStore) SetMany(items map[string][]byte) {
	now := time.Now()
	indexUpdates := make(map[string]map[string]any, len(items))
	indexedFields := s.indexes.ListIndexes()

	// Única transacción física para todos los ítems
	GlobalDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		for key, value := range items {
			record := ItemRecord{Value: value, CreatedAt: now, TTL: 0}
			recordBytes, _ := bson.Marshal(record)
			b.Put([]byte(key), recordBytes)

			if newDataForIndex := extractIndexedValues(value, indexedFields); newDataForIndex != nil {
				indexUpdates[key] = newDataForIndex
			}
		}
		return nil
	})

	// Actualización de índices en RAM
	for key, data := range indexUpdates {
		s.indexes.Update(key, nil, data)
	}
}

// Delete borra usando Update directo para soportar alta velocidad en peticiones individuales
func (s *DiskStore) Delete(key string) {
	oldValue, found := s.Get(key)

	// OPTIMIZADO: Cambiado de Batch a Update para no bloquear esperando otras peticiones
	GlobalDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})

	if found {
		indexedFields := s.indexes.ListIndexes()
		oldDataForIndex := extractIndexedValues(oldValue, indexedFields)
		if oldDataForIndex != nil {
			s.indexes.Remove(key, oldDataForIndex)
		}
	}
}

// DeleteMany borra múltiples ítems en una sola transacción ACID (Bulk Delete)
func (s *DiskStore) DeleteMany(keys []string) {
	oldDatas := make(map[string]map[string]any, len(keys))
	indexedFields := s.indexes.ListIndexes()

	GlobalDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		if b == nil {
			return nil
		}
		for _, k := range keys {
			kb := []byte(k)
			// 🔥 Lectura ultra rápida DENTRO de la misma transacción (Evita N+1 transactions)
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

	for k, oldData := range oldDatas {
		s.indexes.Remove(k, oldData)
	}
}

func (s *DiskStore) Get(key string) ([]byte, bool) {
	var valCopy []byte
	var found bool

	GlobalDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		if b == nil {
			return nil
		}

		recordBytes := b.Get([]byte(key))
		if recordBytes != nil {
			var record ItemRecord
			if err := bson.Unmarshal(recordBytes, &record); err == nil {
				if record.TTL == 0 || time.Since(record.CreatedAt) <= record.TTL {
					valCopy = make([]byte, len(record.Value))
					copy(valCopy, record.Value)
					found = true
				}
			}
		}
		return nil
	})

	return valCopy, found
}

func (s *DiskStore) StreamAll(callback func(key string, value []byte) bool) {
	GlobalDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		if b == nil {
			return nil
		}
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			// 🔥 ZERO-COPY FAST PATH: Leemos el BSON binario sin deserializar el struct
			raw := bson.Raw(v)

			// Verificación de TTL a nivel binario para no perder funcionalidad
			if ttlVal := raw.Lookup("t"); ttlVal.Type != bsontype.Null {
				ttl := ttlVal.Int64() // time.Duration se guarda como int64
				if ttl > 0 {
					createdAt := raw.Lookup("c").Time()
					if time.Since(createdAt) > time.Duration(ttl) {
						continue // Expirado
					}
				}
			}

			// Extraemos los bytes puros del documento interior
			if val := raw.Lookup("v"); val.Type == bsontype.Binary {
				_, data := val.Binary()
				if !callback(string(k), data) {
					break
				}
			}
		}
		return nil
	})
}

func (s *DiskStore) Update(key string, newValue []byte) bool {
	s.Set(key, newValue, 0)
	return true
}

// UpdateMany guarda múltiples parches (ya procesados) en una sola transacción ACID
func (s *DiskStore) UpdateMany(items map[string][]byte) (int, []string) {
	updatedCount := 0
	var failedKeys []string
	indexUpdates := make(map[string]map[string]any, len(items))
	indexedFields := s.indexes.ListIndexes()

	// 1 sola transacción ACID para miles de actualizaciones
	GlobalDB.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.collectionName)
		for key, newValue := range items {
			// Aunque el handler ya hizo el Get, aseguramos que exista en disco
			existing := b.Get([]byte(key))
			if existing == nil {
				failedKeys = append(failedKeys, key)
				continue
			}

			// Nota: Aquí se asume que 'newValue' ya es un ItemRecord completo o un BSON crudo.
			// Para mantener compatibilidad con tu Set, lo envolvemos en ItemRecord.
			var oldRecord ItemRecord
			bson.Unmarshal(existing, &oldRecord) // Conservamos el TTL original

			record := ItemRecord{
				Value:     newValue,
				CreatedAt: oldRecord.CreatedAt,
				TTL:       oldRecord.TTL,
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

	// Actualizamos RAM de golpe
	for key, data := range indexUpdates {
		s.indexes.Update(key, nil, data) // Simplificado: no borra el viejo index, asume sobreescritura
	}

	return updatedCount, failedKeys
}

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
func (s *DiskStore) CleanExpiredItems() bool         { return false }
func (s *DiskStore) LoadData(data map[string][]byte) {}
func (s *DiskStore) GetAll() map[string][]byte       { return nil }
func (s *DiskStore) GetMany(keys []string) map[string][]byte {
	res := make(map[string][]byte)
	for _, k := range keys {
		if val, ok := s.Get(k); ok {
			res[k] = val
		}
	}
	return res
}
