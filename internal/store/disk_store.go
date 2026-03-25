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

// SetMany guarda múltiples ítems y sus índices insertando en paralelo por partición.
func (s *DiskStore) SetMany(items map[string][]byte) (int, int) {
	now := time.Now().UTC()
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

	var insertedCount, duplicateCount int32
	var wg sync.WaitGroup

	// 2. Escribir en disco por cada Shard en PARALELO
	for shardID, sItems := range shardItems {
		wg.Add(1)
		go func(sID int, itemsForShard map[string][]byte) {
			defer wg.Done()
			db := GlobalDBs[sID]
			var localInserted, localDuplicated int32

			db.Update(func(tx *bbolt.Tx) error {
				b, _ := tx.CreateBucketIfNotExists(s.collectionName)

				for key, value := range itemsForShard {
					// Verificación O(1) en RAM/mmap (¡Muchísimo más rápido que un db.View() externo!)
					if b.Get([]byte(key)) != nil {
						localDuplicated++
						continue
					}

					// Serialización optimizada con bson.D conservando la Versión
					recordBytes, _ := bson.Marshal(bson.D{
						{Key: "v", Value: value},
						{Key: "c", Value: now},
						{Key: "ver", Value: uint64(1)}, // Asignamos la primera versión
					})
					b.Put([]byte(key), recordBytes)

					newDataForIndex := extractIndexedValues(value, indexedFields)
					for field, val := range newDataForIndex {
						idxKey := encodeIndexKey(val, key)
						if idxKey != nil {
							idxB, _ := tx.CreateBucketIfNotExists([]byte("_idx_" + string(s.collectionName) + "_" + field))
							idxB.Put(idxKey, []byte{})
						}
					}
					localInserted++
				}
				return nil
			})

			atomic.AddInt32(&insertedCount, localInserted)
			atomic.AddInt32(&duplicateCount, localDuplicated)
		}(shardID, sItems)
	}

	wg.Wait()
	return int(insertedCount), int(duplicateCount)
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

// DeleteMany borra múltiples ítems y sus índices agrupándolos por partición (Optimización Secuencial).
func (s *DiskStore) DeleteMany(keys []string) {
	indexedFields := s.indexes.ListIndexes()

	// 1. Enrutamiento por Shard (Igual que antes)
	shardKeys := make(map[int][]string)
	for _, key := range keys {
		shardID := GetShardID(key, TotalShards)
		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

	var wg sync.WaitGroup

	// 2. Procesamiento concurrente por Shard
	for shardID, sKeys := range shardKeys {
		wg.Add(1)
		go func(sID int, keysForShard []string) {
			defer wg.Done()
			db := GlobalDBs[sID]

			db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket(s.collectionName)
				if b == nil {
					return nil
				}

				// Diccionarios para agrupar TODOS los índices a borrar antes de tocar los buckets
				indexDeletes := make(map[string][][]byte)

				// Fase 1: Recolección y Eliminación del Documento Principal
				for _, k := range keysForShard {
					keyBytes := []byte(k)
					if oldVal := b.Get(keyBytes); oldVal != nil {
						var record ItemRecord
						// Fast-path: Unmarshal mínimo si es posible.
						// Para este ejemplo, mantendremos Unmarshal para asegurar que extraemos bien los datos indexados.
						if bson.Unmarshal(oldVal, &record) == nil {
							oldData := extractIndexedValues(record.Value, indexedFields)
							for field, val := range oldData {
								if idxKey := encodeIndexKey(val, k); idxKey != nil {
									indexDeletes[field] = append(indexDeletes[field], idxKey)
								}
							}
						}
					}
					b.Delete(keyBytes) // Borrado del dato crudo
				}

				// Fase 2: Borrado en Lote de Índices
				// Al hacer esto separado, bbolt no brinca entre el bucket principal y los de índices.
				// Esto maximiza la retención en caché L1/L2 del procesador.
				for field, idxKeys := range indexDeletes {
					idxBName := []byte("_idx_" + string(s.collectionName) + "_" + field)
					idxB := tx.Bucket(idxBName)
					if idxB != nil {
						for _, idxKey := range idxKeys {
							idxB.Delete(idxKey)
						}
					}
				}
				return nil
			})
		}(shardID, sKeys)
	}
	wg.Wait()
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

// GetMany recupera múltiples ítems agrupando las consultas y ejecutándolas concurrentemente por partición.
func (s *DiskStore) GetMany(keys []string) map[string][]byte {
	res := make(map[string][]byte, len(keys))
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Enrutamiento de llaves a su Shard correspondiente
	shardKeys := make(map[int][]string)
	for _, key := range keys {
		shardID := GetShardID(key, TotalShards)
		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

	// Consultar los Shards en paralelo en lugar de secuencialmente
	for shardID, sKeys := range shardKeys {
		wg.Add(1)
		go func(sID int, keysToFetch []string) {
			defer wg.Done()
			db := GlobalDBs[sID]
			localRes := make(map[string][]byte)

			db.View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(s.collectionName)
				if b == nil {
					return nil
				}
				for _, k := range keysToFetch {
					if recordBytes := b.Get([]byte(k)); recordBytes != nil {
						raw := bson.Raw(recordBytes)
						if val := raw.Lookup("v"); val.Type == bsontype.Binary {
							_, data := val.Binary()
							// Copia segura de la memoria mmap de bbolt
							valCopy := make([]byte, len(data))
							copy(valCopy, data)
							localRes[k] = valCopy
						}
					}
				}
				return nil
			})

			// Volcar resultados locales al mapa general de forma segura
			if len(localRes) > 0 {
				mu.Lock()
				for k, v := range localRes {
					res[k] = v
				}
				mu.Unlock()
			}
		}(shardID, sKeys)
	}

	wg.Wait()
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

// UpdateMany actualiza múltiples ítems e índices ruteando por partición (Optimización Secuencial).
func (s *DiskStore) UpdateMany(items map[string][]byte) (int, []string) {
	var updatedCount int32
	var failedKeys []string
	var mu sync.Mutex // Para proteger failedKeys en concurrencia

	indexedFields := s.indexes.ListIndexes()

	shardItems := make(map[int]map[string][]byte)
	for key, value := range items {
		shardID := GetShardID(key, TotalShards)
		if shardItems[shardID] == nil {
			shardItems[shardID] = make(map[string][]byte)
		}
		shardItems[shardID][key] = value
	}

	var wg sync.WaitGroup

	for shardID, sItems := range shardItems {
		wg.Add(1)
		go func(sID int, itemsForShard map[string][]byte) {
			defer wg.Done()
			db := GlobalDBs[sID]

			var localUpdated int32
			var localFailed []string

			db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket(s.collectionName)
				if b == nil {
					return nil
				}

				// Agrupadores de índices
				indexDeletes := make(map[string][][]byte)
				indexInserts := make(map[string][][]byte)

				for key, newValue := range itemsForShard {
					keyBytes := []byte(key)
					existing := b.Get(keyBytes)
					if existing == nil {
						localFailed = append(localFailed, key)
						continue
					}

					var oldRecord ItemRecord
					bson.Unmarshal(existing, &oldRecord)

					// Incrementamos la versión cuidando el control de MVCC
					recordBytes, _ := bson.Marshal(ItemRecord{
						Value:     newValue,
						CreatedAt: oldRecord.CreatedAt,
						Version:   oldRecord.Version + 1, // Incrementamos la versión anterior
					})

					if err := b.Put(keyBytes, recordBytes); err == nil {
						localUpdated++

						oldData := extractIndexedValues(oldRecord.Value, indexedFields)
						newData := extractIndexedValues(newValue, indexedFields)

						for field := range oldData {
							if oldData[field] != newData[field] {
								if oldK := encodeIndexKey(oldData[field], key); oldK != nil {
									indexDeletes[field] = append(indexDeletes[field], oldK)
								}
							}
						}
						for field, val := range newData {
							if oldData[field] != val {
								if newK := encodeIndexKey(val, key); newK != nil {
									indexInserts[field] = append(indexInserts[field], newK)
								}
							}
						}
					} else {
						localFailed = append(localFailed, key)
					}
				}

				// Procesar purgas de índices masivas
				for field, keys := range indexDeletes {
					if idxB := tx.Bucket([]byte("_idx_" + string(s.collectionName) + "_" + field)); idxB != nil {
						for _, k := range keys {
							idxB.Delete(k)
						}
					}
				}

				// Procesar inserciones de índices masivas
				for field, keys := range indexInserts {
					idxB, _ := tx.CreateBucketIfNotExists([]byte("_idx_" + string(s.collectionName) + "_" + field))
					for _, k := range keys {
						idxB.Put(k, []byte{})
					}
				}

				return nil
			})

			atomic.AddInt32(&updatedCount, localUpdated)
			if len(localFailed) > 0 {
				mu.Lock()
				failedKeys = append(failedKeys, localFailed...)
				mu.Unlock()
			}
		}(shardID, sItems)
	}

	wg.Wait()
	return int(updatedCount), failedKeys
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

type streamItem struct {
	key []byte
}

// StreamByIndex itera sobre los índices utilizando un K-Way Merge concurrente para garantizar
// un ordenamiento global perfecto a través de todas las particiones (Shards).
func (s *DiskStore) StreamByIndex(field string, desc bool, cb func(key string) bool) bool {
	if !s.HasIndex(field) {
		return false
	}

	idxBucketName := []byte("_idx_" + string(s.collectionName) + "_" + field)

	// Canales para el K-Way Merge: uno por cada Shard
	itemsCh := make([]chan streamItem, TotalShards)
	cancelCh := make(chan struct{})
	var wg sync.WaitGroup

	// 1. Iniciar lectura concurrente en todos los Shards
	for i := 0; i < TotalShards; i++ {
		itemsCh[i] = make(chan streamItem, 64) // Buffer para pre-carga
		wg.Add(1)

		go func(shardID int, ch chan streamItem) {
			defer wg.Done()
			defer close(ch)

			GlobalDBs[shardID].View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(idxBucketName)
				if b == nil {
					return nil
				}
				c := b.Cursor()

				var k []byte
				if desc {
					k, _ = c.Last()
				} else {
					k, _ = c.First()
				}

				for k != nil {
					// Comprobar si el coordinador pidió abortar (ej. se cumplió el LIMIT)
					select {
					case <-cancelCh:
						return nil
					default:
					}

					// bbolt recicla la memoria de 'k', debemos copiarla
					keyCopy := make([]byte, len(k))
					copy(keyCopy, k)

					// Enviar al coordinador
					select {
					case ch <- streamItem{key: keyCopy}:
					case <-cancelCh:
						return nil
					}

					if desc {
						k, _ = c.Prev()
					} else {
						k, _ = c.Next()
					}
				}
				return nil
			})
		}(i, itemsCh[i])
	}

	// 2. Coordinador K-Way Merge
	currentItems := make([]*streamItem, TotalShards)
	activeShards := TotalShards

	// Cargar el primer elemento de cada Shard
	for i := 0; i < TotalShards; i++ {
		item, ok := <-itemsCh[i]
		if ok {
			currentItems[i] = &item
		} else {
			currentItems[i] = nil
			activeShards--
		}
	}

	keepGoing := true
	for activeShards > 0 && keepGoing {
		bestIdx := -1

		// Encontrar la llave ganadora entre los Shards activos
		for i := 0; i < TotalShards; i++ {
			if currentItems[i] == nil {
				continue
			}
			if bestIdx == -1 {
				bestIdx = i
				continue
			}

			// bytes.Compare funciona perfectamente porque los bytes de índice
			// de LunaDB están diseñados para ser lexicográficamente correctos.
			cmp := bytes.Compare(currentItems[i].key, currentItems[bestIdx].key)
			if desc {
				if cmp > 0 { // Descendente: buscamos el mayor
					bestIdx = i
				}
			} else {
				if cmp < 0 { // Ascendente: buscamos el menor
					bestIdx = i
				}
			}
		}

		if bestIdx == -1 {
			break
		}

		// Procesar el elemento ganador
		docID := decodeDocID(currentItems[bestIdx].key)
		if !cb(docID) {
			keepGoing = false
			break
		}

		// Extraer el siguiente elemento SOLO del Shard que acaba de ganar
		item, ok := <-itemsCh[bestIdx]
		if ok {
			currentItems[bestIdx] = &item
		} else {
			currentItems[bestIdx] = nil
			activeShards--
		}
	}

	// 3. Limpieza y cierre seguro
	close(cancelCh) // Avisa a las gorutinas que detengan la lectura
	wg.Wait()       // Espera a que todas las transacciones View se cierren

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
