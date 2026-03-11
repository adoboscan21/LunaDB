package store

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"lunadb/internal/globalconst"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// extractIndexedValues extrae SOLAMENTE los campos necesarios para los índices directamente de los bytes.
func extractIndexedValues(rawDoc []byte, indexedFields []string) map[string]any {
	if len(rawDoc) == 0 || len(indexedFields) == 0 {
		return nil
	}
	raw := bson.Raw(rawDoc)
	result := make(map[string]any, len(indexedFields))
	for _, field := range indexedFields {
		keys := strings.Split(field, ".")
		val, err := raw.LookupErr(keys...)
		if err == nil && val.Type != bsontype.Null {
			switch val.Type {
			case bsontype.Double:
				result[field] = val.Double()
			case bsontype.String:
				result[field] = val.StringValue()
			case bsontype.Int32:
				result[field] = int64(val.Int32()) // Normalizamos enteros
			case bsontype.Int64:
				result[field] = val.Int64()
			case bsontype.Boolean:
				result[field] = val.Boolean()
			case bsontype.DateTime:
				result[field] = val.Time()
			default:
				result[field] = val.String()
			}
		}
	}
	return result
}

// valueToFloat64 is a helper to safely convert various numeric types to float64.
func valueToFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// --- B-Tree Indexing Structures ---

const btreeDegree = 32

// NumericKey implements the item for the numeric B-Tree.
type NumericKey struct {
	Value float64
	Keys  map[string]struct{}
}

// StringKey implements the item for the string B-Tree.
type StringKey struct {
	Value string
	Keys  map[string]struct{}
}

// numericLess provides the comparison logic for NumericKey items.
func numericLess(a, b NumericKey) bool {
	return a.Value < b.Value
}

// stringLess provides the comparison logic for StringKey items.
func stringLess(a, b StringKey) bool {
	return a.Value < b.Value
}

// Index now contains two B-Trees, one for each supported data type.
type Index struct {
	numericTree *btree.BTreeG[NumericKey]
	stringTree  *btree.BTreeG[StringKey]
}

// NewIndex creates a new index structure with initialized B-Trees.
func NewIndex() *Index {
	return &Index{
		numericTree: btree.NewG[NumericKey](btreeDegree, numericLess),
		stringTree:  btree.NewG[StringKey](btreeDegree, stringLess),
	}
}

// --- IndexManager for B-Trees ---

// IndexManager manages all indexes for a single InMemStore.
type IndexManager struct {
	mu      sync.RWMutex
	indexes map[string]*Index
}

// NewIndexManager creates a new index manager.
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
	}
}

// CreateIndex initializes a new B-Tree index for a given field.
func (im *IndexManager) CreateIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; !exists {
		im.indexes[field] = NewIndex()
		slog.Info("B-Tree Index created", "field", field)
	}
}

// DeleteIndex removes an index for a given field.
func (im *IndexManager) DeleteIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; exists {
		delete(im.indexes, field)
		slog.Info("Index deleted", "field", field)
	}
}

// ListIndexes returns the names of all indexed fields.
func (im *IndexManager) ListIndexes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()
	indexedFields := make([]string, 0, len(im.indexes))
	for field := range im.indexes {
		indexedFields = append(indexedFields, field)
	}
	return indexedFields
}

// addToIndex adds a document key to an index for a specific value.
func (im *IndexManager) addToIndex(index *Index, docKey string, value any) {
	if fVal, ok := valueToFloat64(value); ok {
		key := NumericKey{Value: fVal}
		item, found := index.numericTree.Get(key)
		if !found {
			item = NumericKey{Value: fVal, Keys: make(map[string]struct{})}
		}
		item.Keys[docKey] = struct{}{}
		index.numericTree.ReplaceOrInsert(item)
	} else if sVal, ok := value.(string); ok {
		key := StringKey{Value: sVal}
		item, found := index.stringTree.Get(key)
		if !found {
			item = StringKey{Value: sVal, Keys: make(map[string]struct{})}
		}
		item.Keys[docKey] = struct{}{}
		index.stringTree.ReplaceOrInsert(item)
	}
}

// removeFromIndex removes a document key from an index.
func (im *IndexManager) removeFromIndex(index *Index, docKey string, value any) {
	if fVal, ok := valueToFloat64(value); ok {
		key := NumericKey{Value: fVal}
		if item, found := index.numericTree.Get(key); found {
			delete(item.Keys, docKey)
			if len(item.Keys) == 0 {
				index.numericTree.Delete(item)
			} else {
				index.numericTree.ReplaceOrInsert(item)
			}
		}
	} else if sVal, ok := value.(string); ok {
		key := StringKey{Value: sVal}
		if item, found := index.stringTree.Get(key); found {
			delete(item.Keys, docKey)
			if len(item.Keys) == 0 {
				index.stringTree.Delete(item)
			} else {
				index.stringTree.ReplaceOrInsert(item)
			}
		}
	}
}

// Update updates the indexes for a given document.
func (im *IndexManager) Update(docKey string, oldData, newData map[string]any) {
	im.mu.Lock()
	defer im.mu.Unlock()

	if len(im.indexes) == 0 {
		return
	}

	for field, index := range im.indexes {
		oldVal, oldOk := oldData[field]
		newVal, newOk := newData[field]

		if oldOk && newOk && oldVal == newVal {
			continue
		}

		if oldOk {
			im.removeFromIndex(index, docKey, oldVal)
		}
		if newOk {
			im.addToIndex(index, docKey, newVal)
		}
	}
}

// Remove removes a document from all indexes.
func (im *IndexManager) Remove(docKey string, data map[string]any) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if data == nil || len(im.indexes) == 0 {
		return
	}
	for field, index := range im.indexes {
		if val, ok := data[field]; ok {
			im.removeFromIndex(index, docKey, val)
		}
	}
}

// Lookup performs an equality lookup on an index.
func (im *IndexManager) Lookup(field string, value any) ([]string, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false
	}

	var foundKeys map[string]struct{}
	if fVal, ok := valueToFloat64(value); ok {
		if item, found := index.numericTree.Get(NumericKey{Value: fVal}); found {
			foundKeys = item.Keys
		}
	} else if sVal, ok := value.(string); ok {
		if item, found := index.stringTree.Get(StringKey{Value: sVal}); found {
			foundKeys = item.Keys
		}
	}

	if foundKeys == nil {
		return []string{}, true
	}

	keys := make([]string, 0, len(foundKeys))
	for k := range foundKeys {
		keys = append(keys, k)
	}
	return keys, true
}

// LookupRange performs a range scan on a B-Tree index.
func (im *IndexManager) LookupRange(field string, low, high any, lowInclusive, highInclusive bool) ([]string, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false
	}

	unionKeys := make(map[string]struct{})
	var isNumericQuery bool
	if low != nil {
		if _, ok := valueToFloat64(low); ok {
			isNumericQuery = true
		}
	} else if high != nil {
		if _, ok := valueToFloat64(high); ok {
			isNumericQuery = true
		}
	}

	if isNumericQuery {
		var lowKey, highKey NumericKey
		hasLowBound, hasHighBound := low != nil, high != nil
		if hasLowBound {
			lowKey.Value, _ = valueToFloat64(low)
		}
		if hasHighBound {
			highKey.Value, _ = valueToFloat64(high)
		}

		iterator := func(item NumericKey) bool {
			if hasHighBound {
				if item.Value > highKey.Value {
					return false
				}
				if !highInclusive && item.Value == highKey.Value {
					return false
				}
			}
			for k := range item.Keys {
				unionKeys[k] = struct{}{}
			}
			return true
		}

		startKey := lowKey
		if !hasLowBound {
			if minItem, ok := index.numericTree.Min(); ok {
				startKey = minItem
			} else {
				return []string{}, true
			}
		}

		index.numericTree.AscendGreaterOrEqual(startKey, iterator)

		if hasLowBound && !lowInclusive {
			if item, found := index.numericTree.Get(lowKey); found {
				for k := range item.Keys {
					delete(unionKeys, k)
				}
			}
		}

	} else {
		var lowKey, highKey StringKey
		hasLowBound, hasHighBound := low != nil, high != nil
		if hasLowBound {
			lowKey.Value, _ = low.(string)
		}
		if hasHighBound {
			highKey.Value, _ = high.(string)
		}

		iterator := func(item StringKey) bool {
			if hasHighBound {
				if item.Value > highKey.Value {
					return false
				}
				if !highInclusive && item.Value == highKey.Value {
					return false
				}
			}
			for k := range item.Keys {
				unionKeys[k] = struct{}{}
			}
			return true
		}

		startKey := lowKey
		if !hasLowBound {
			if minItem, ok := index.stringTree.Min(); ok {
				startKey = minItem
			} else {
				return []string{}, true
			}
		}

		index.stringTree.AscendGreaterOrEqual(startKey, iterator)

		if hasLowBound && !lowInclusive {
			if item, found := index.stringTree.Get(lowKey); found {
				for k := range item.Keys {
					delete(unionKeys, k)
				}
			}
		}
	}

	finalKeys := make([]string, 0, len(unionKeys))
	for k := range unionKeys {
		finalKeys = append(finalKeys, k)
	}
	return finalKeys, true
}

// HasIndex checks if an index exists for a given field.
func (im *IndexManager) HasIndex(field string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, exists := im.indexes[field]
	return exists
}

type Item struct {
	Value         []byte
	IndexedValues map[string]any
	CreatedAt     time.Time
	TTL           time.Duration
}

// Shard represents a segment of the in-memory store.
type Shard struct {
	data          map[string]Item
	mu            sync.RWMutex
	keyLocks      map[string]string
	pendingWrites map[string]map[string]Item
}

// DataStore defines the interface for data storage and retrieval.
type DataStore interface {
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	GetMany(keys []string) map[string][]byte
	Delete(key string)
	GetAll() map[string][]byte
	StreamAll(callback func(key string, value []byte) bool)
	LoadData(data map[string][]byte)
	CleanExpiredItems() bool
	Size() int
	CreateIndex(field string)
	DeleteIndex(field string)
	ListIndexes() []string
	HasIndex(field string) bool
	Lookup(field string, value any) ([]string, bool)
	LookupRange(field string, low, high any, lowInclusive, highInclusive bool) ([]string, bool)
	StreamByIndex(field string, descending bool, callback func(key string) bool) bool
	Update(key string, newValue []byte) bool
	GetDistinctValues(field string) ([]any, bool)
	GetGroupedCount(field string) (map[any]int, bool)
}

// InMemStore implements DataStore for in-memory storage, with sharding and indexing.
type InMemStore struct {
	shards    []*Shard
	numShards int
	indexes   *IndexManager
}

// NewInMemStoreWithShards creates a new InMemStore with a specified number of shards.
func NewInMemStoreWithShards(numShards int) *InMemStore {
	s := &InMemStore{
		shards:    make([]*Shard, numShards),
		numShards: numShards,
		indexes:   NewIndexManager(),
	}
	for i := range numShards {
		s.shards[i] = &Shard{
			data:          make(map[string]Item),
			keyLocks:      make(map[string]string),
			pendingWrites: make(map[string]map[string]Item),
		}
	}
	slog.Info("InMemStore initialized", "num_shards", numShards)
	return s
}

// getShard determines which shard a given key belongs to.
func (s *InMemStore) getShard(key string) *Shard {
	h := fnv.New64a()
	h.Write([]byte(key))
	shardIndex := h.Sum64() % uint64(s.numShards)
	return s.shards[shardIndex]
}

// getShardIndex for logging purposes.
func (s *InMemStore) getShardIndex(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64() % uint64(s.numShards)
}

// Set saves a key-value pair and updates any relevant indexes.
func (s *InMemStore) Set(key string, value []byte, ttl time.Duration) {
	// 1. Extraer índices del NUEVO valor ANTES de bloquear (Cero contención)
	indexedFields := s.indexes.ListIndexes()
	newDataForIndex := extractIndexedValues(value, indexedFields)

	shard := s.getShard(key)
	shard.mu.Lock()

	if ownerTxID, isLocked := shard.keyLocks[key]; isLocked {
		slog.Warn("Set operation rejected: key is locked by an active transaction", "key", key, "txID", ownerTxID)
		shard.mu.Unlock() // Desbloquear explícitamente en caso de error
		return
	}

	oldItem, isUpdate := shard.data[key]

	createdAt := time.Now()
	if isUpdate {
		createdAt = oldItem.CreatedAt
	}
	newItem := Item{
		Value:         value,
		IndexedValues: newDataForIndex, // Guardamos la caché para futuros Deletes/Updates
		CreatedAt:     createdAt,
		TTL:           ttl,
	}

	shard.data[key] = newItem

	// 2. LIBERAMOS EL LOCK TEMPRANO: El Shard vuelve a estar libre para otros hilos
	shard.mu.Unlock()

	// 3. Actualizamos los árboles B-Tree de forma asíncrona al Shard
	var oldDataForIndex map[string]any
	if isUpdate {
		// ¡MAGIA! Ya no parseamos el BSON antiguo, usamos la caché instantánea en O(1)
		oldDataForIndex = oldItem.IndexedValues
	}

	if oldDataForIndex != nil || newDataForIndex != nil {
		s.indexes.Update(key, oldDataForIndex, newDataForIndex)
	}

	slog.Debug("Item set", "shard_id", s.getShardIndex(key), "key", key, "is_update", isUpdate)
}

// Get retrieves a value from the store by its key.
func (s *InMemStore) Get(key string) ([]byte, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	item, found := shard.data[key]
	if !found {
		slog.Debug("Item get", "shard_id", s.getShardIndex(key), "key", key, "status", "not_found")
		return nil, false
	}

	if item.TTL > 0 && time.Since(item.CreatedAt) > item.TTL {
		slog.Debug("Item get", "shard_id", s.getShardIndex(key), "key", key, "status", "expired")
		return nil, false
	}

	slog.Debug("Item get", "shard_id", s.getShardIndex(key), "key", key, "status", "found")
	return item.Value, true
}

// GetMany retrieves multiple keys concurrently by grouping them by shard.
func (s *InMemStore) GetMany(keys []string) map[string][]byte {
	if len(keys) == 0 {
		return make(map[string][]byte)
	}

	keysByShard := make([][]string, s.numShards)
	for _, key := range keys {
		h := fnv.New64a()
		h.Write([]byte(key))
		shardIndex := h.Sum64() % uint64(s.numShards)
		keysByShard[shardIndex] = append(keysByShard[shardIndex], key)
	}

	resultsChan := make(chan map[string][]byte, s.numShards)
	var wg sync.WaitGroup
	now := time.Now()

	for i, shardKeys := range keysByShard {
		if len(shardKeys) > 0 {
			wg.Add(1)
			go func(shardIndex int, keysInShard []string) {
				defer wg.Done()
				shard := s.shards[shardIndex]
				shardResults := make(map[string][]byte, len(keysInShard))
				shard.mu.RLock()
				for _, key := range keysInShard {
					if item, found := shard.data[key]; found {
						if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
							shardResults[key] = item.Value
						}
					}
				}
				shard.mu.RUnlock()
				resultsChan <- shardResults
			}(i, shardKeys)
		}
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	finalResults := make(map[string][]byte, len(keys))
	for shardResults := range resultsChan {
		maps.Copy(finalResults, shardResults)
	}

	return finalResults
}

// Delete removes a key-value pair and updates any relevant indexes.
func (s *InMemStore) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()

	if ownerTxID, isLocked := shard.keyLocks[key]; isLocked {
		slog.Warn("Delete operation rejected: key is locked by an active transaction", "key", key, "txID", ownerTxID)
		shard.mu.Unlock()
		return
	}

	var dataForIndex map[string]any
	if item, exists := shard.data[key]; exists {
		// ¡MAGIA! Recuperamos los valores indexados al instante sin tocar el BSON
		dataForIndex = item.IndexedValues
		delete(shard.data, key)
	}

	// LIBERAMOS EL LOCK TEMPRANO
	shard.mu.Unlock()

	// Borramos del B-Tree (El IndexManager ya tiene sus propios locks finos)
	if dataForIndex != nil {
		s.indexes.Remove(key, dataForIndex)
	}

	slog.Debug("Item deleted", "shard_id", s.getShardIndex(key), "key", key)
}

// GetAll returns a copy of all non-expired data from ALL shards for persistence.
func (s *InMemStore) GetAll() map[string][]byte {
	snapshotData := make(map[string][]byte)
	now := time.Now()

	for _, shard := range s.shards {
		shard.mu.RLock()
		for k, item := range shard.data {
			if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
				copyValue := make([]byte, len(item.Value))
				copy(copyValue, item.Value)
				snapshotData[k] = copyValue
			}
		}
		shard.mu.RUnlock()
	}
	slog.Debug("Snapshot data combined", "num_shards", s.numShards, "total_items", len(snapshotData))
	return snapshotData
}

// LoadData loads data into the store across its shards from a persistent source.
func (s *InMemStore) LoadData(data map[string][]byte) {
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.data = make(map[string]Item)
		shard.mu.Unlock()
	}
	slog.Info("All shards cleared for data load")

	for k, v := range data {
		shard := s.getShard(k)
		shard.mu.Lock()
		shard.data[k] = Item{
			Value:     v,
			CreatedAt: time.Now(),
			TTL:       0,
		}
		shard.mu.Unlock()
	}
	slog.Info("Data loaded into shards", "num_shards", s.numShards, "total_keys", len(data))
}

// CleanExpiredItems iterates through each shard and physically deletes expired items.
func (s *InMemStore) CleanExpiredItems() bool {
	totalDeletedCount := 0
	now := time.Now()
	wasModified := false

	// EXTRAER LOS ÍNDICES UNA SOLA VEZ AL INICIO DE LA LIMPIEZA
	indexedFields := s.indexes.ListIndexes()

	for i, shard := range s.shards {
		shard.mu.RLock()
		var expiredKeys []string
		for key, item := range shard.data {
			if item.TTL > 0 && now.After(item.CreatedAt.Add(item.TTL)) {
				expiredKeys = append(expiredKeys, key)
			}
		}
		shard.mu.RUnlock()

		if len(expiredKeys) == 0 {
			continue
		}

		shard.mu.Lock()
		deletedInShard := 0
		for _, key := range expiredKeys {
			if item, exists := shard.data[key]; exists && item.TTL > 0 && now.After(item.CreatedAt.Add(item.TTL)) {
				data := extractIndexedValues(item.Value, indexedFields) // Zero-Copy
				if data != nil {
					s.indexes.Remove(key, data)
				}
				delete(shard.data, key)
				deletedInShard++
				wasModified = true
			}
		}
		shard.mu.Unlock()

		if deletedInShard > 0 {
			totalDeletedCount += deletedInShard
			slog.Info("TTL cleaner removed expired items from shard", "shard_id", i, "count", deletedInShard)
		}
	}

	if totalDeletedCount > 0 {
		slog.Info("TTL cleaner finished run", "total_removed", totalDeletedCount)
	} else {
		slog.Debug("TTL cleaner run complete: no items to remove")
	}
	return wasModified
}

// Size returns the total number of items in the store across all shards.
func (s *InMemStore) Size() int {
	total := 0
	for _, shard := range s.shards {
		shard.mu.RLock()
		total += len(shard.data)
		shard.mu.RUnlock()
	}
	return total
}

// --- Indexing method implementations for InMemStore ---

// CreateIndex creates an index on a field and backfills it with existing data.
func (s *InMemStore) CreateIndex(field string) {
	if s.HasIndex(field) {
		slog.Debug("Index creation skipped: already exists", "field", field)
		return
	}
	s.indexes.CreateIndex(field)

	slog.Info("Backfilling index", "field", field)
	allData := s.GetAll()
	count := 0
	indexedFields := s.indexes.ListIndexes() // Obtenemos la lista de índices actualizados

	for key, value := range allData {
		if data := extractIndexedValues(value, indexedFields); data != nil { // Zero-Copy
			s.indexes.Update(key, nil, data)
			count++
		}
	}
	slog.Info("Index backfill complete", "field", field, "item_count", count)
}

// DeleteIndex removes an index from the store.
func (s *InMemStore) DeleteIndex(field string) {
	s.indexes.DeleteIndex(field)
}

// ListIndexes returns a list of indexed fields.
func (s *InMemStore) ListIndexes() []string {
	return s.indexes.ListIndexes()
}

// HasIndex checks if an index exists on a field.
func (s *InMemStore) HasIndex(field string) bool {
	return s.indexes.HasIndex(field)
}

// Lookup uses the index manager to find document keys for an exact value.
func (s *InMemStore) Lookup(field string, value any) ([]string, bool) {
	return s.indexes.Lookup(field, value)
}

// LookupRange uses the index manager to find document keys within a range.
func (s *InMemStore) LookupRange(field string, low, high any, lowInclusive, highInclusive bool) ([]string, bool) {
	return s.indexes.LookupRange(field, low, high, lowInclusive, highInclusive)
}

// CollectionPersister defines the interface for persistence operations specific to collections.
type CollectionPersister interface {
	SaveCollectionData(collectionName string, s DataStore) error
	DeleteCollectionFile(collectionName string) error
}

type colTaskType int

const (
	taskSave colTaskType = iota
	taskDelete
)

type asyncTask struct {
	TaskType       colTaskType
	CollectionName string
	Collection     DataStore // Se usa si TaskType es taskSave
}

// CollectionManager manages multiple named InMemStore instances, each representing a collection.
type CollectionManager struct {
	collections map[string]DataStore
	mu          sync.RWMutex
	persister   CollectionPersister
	taskQueue   chan asyncTask
	quit        chan struct{}
	wg          sync.WaitGroup
	numShards   int
	fileLocks   map[string]*sync.Mutex
	fileLocksMu sync.RWMutex
}

// NewCollectionManager creates a new instance of CollectionManager.
func NewCollectionManager(persister CollectionPersister, numShards int) *CollectionManager {
	cm := &CollectionManager{
		collections: make(map[string]DataStore),
		persister:   persister,
		taskQueue:   make(chan asyncTask, 100),
		quit:        make(chan struct{}),
		numShards:   numShards,
		fileLocks:   make(map[string]*sync.Mutex),
	}
	cm.StartAsyncWorker()
	return cm
}

// StartAsyncWorker launches a background goroutine to process tasks in order.
// OPTIMIZACIÓN: Añadido "Debouncing" (agrupación) para evitar I/O Thrashing en disco.
func (cm *CollectionManager) StartAsyncWorker() {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		slog.Info("Async collection worker started with I/O Debouncing.")

		// Mapa para agrupar las peticiones de guardado pendientes
		pendingSaves := make(map[string]DataStore)
		debounceDuration := 500 * time.Millisecond // Ventana de espera para agrupar I/O
		timer := time.NewTimer(debounceDuration)
		if !timer.Stop() {
			<-timer.C
		}
		timerActive := false

		// Función auxiliar para volcar todo lo agrupado a disco
		savePending := func() {
			for colName, colStore := range pendingSaves {
				fileLock := cm.GetFileLock(colName)
				fileLock.Lock()
				if err := cm.persister.SaveCollectionData(colName, colStore); err != nil {
					slog.Error("Error saving collection from async task", "collection", colName, "error", err)
				}
				fileLock.Unlock()
			}
			// Limpiamos el mapa después de guardar
			pendingSaves = make(map[string]DataStore)
		}

		for {
			select {
			case task, ok := <-cm.taskQueue:
				if !ok {
					slog.Info("Async task queue closed, stopping worker.")
					savePending() // Guardamos lo que haya quedado pendiente
					return
				}

				if task.TaskType == taskSave {
					// Agrupamos la petición. Si mandan 100 updates rápidos, solo se guarda 1 vez al final.
					pendingSaves[task.CollectionName] = task.Collection
					if !timerActive {
						timer.Reset(debounceDuration)
						timerActive = true
					}
				} else if task.TaskType == taskDelete {
					// Los borrados son inmediatos y cancelan guardados pendientes de esa colección
					delete(pendingSaves, task.CollectionName)

					fileLock := cm.GetFileLock(task.CollectionName)
					fileLock.Lock()
					if err := cm.persister.DeleteCollectionFile(task.CollectionName); err != nil {
						slog.Error("Error deleting collection file from async task", "collection", task.CollectionName, "error", err)
					}
					fileLock.Unlock()
				}

			case <-timer.C:
				// Se cumplió la ventana de 500ms sin nuevas peticiones, escribimos a disco de golpe
				timerActive = false
				savePending()

			case <-cm.quit:
				slog.Info("Async worker received quit signal. Draining queue...")
				savePending() // Guardamos el estado actual agrupado

				// Vaciamos la cola restante en caso de apagado repentino
				for len(cm.taskQueue) > 0 {
					task := <-cm.taskQueue
					fileLock := cm.GetFileLock(task.CollectionName)
					fileLock.Lock()
					if task.TaskType == taskDelete {
						_ = cm.persister.DeleteCollectionFile(task.CollectionName)
					} else if task.TaskType == taskSave {
						_ = cm.persister.SaveCollectionData(task.CollectionName, task.Collection)
					}
					fileLock.Unlock()
				}
				slog.Info("Async collection worker stopped.")
				return
			}
		}
	}()
}

// Wait blocks until all outstanding tasks are complete and the worker stops.
func (cm *CollectionManager) Wait() {
	close(cm.quit)
	cm.wg.Wait()
}

// EnqueueSaveTask adds a collection save request to the asynchronous queue.
func (cm *CollectionManager) EnqueueSaveTask(collectionName string, col DataStore) {
	task := asyncTask{
		TaskType:       taskSave,
		CollectionName: collectionName,
		Collection:     col,
	}

	select {
	case cm.taskQueue <- task:
		slog.Debug("Save task enqueued", "collection", collectionName)
	default:
		slog.Warn("Task queue is full, dropping save task", "collection", collectionName)
	}
}

// EnqueueDeleteTask adds a collection delete request to the asynchronous queue.
func (cm *CollectionManager) EnqueueDeleteTask(collectionName string) {
	task := asyncTask{
		TaskType:       taskDelete,
		CollectionName: collectionName,
	}

	select {
	case cm.taskQueue <- task:
		slog.Debug("Delete task enqueued", "collection", collectionName)
	default:
		slog.Warn("Task queue is full, dropping delete task", "collection", collectionName)
	}
}

// GetCollection retrieves an existing collection (InMemStore) by name, or creates a new one.
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

	newCol := NewInMemStoreWithShards(cm.numShards)
	newCol.CreateIndex(globalconst.ID)
	cm.collections[name] = newCol
	slog.Info("Collection created", "name", name, "num_shards", cm.numShards)
	return newCol
}

// DeleteCollection removes a collection entirely from the manager.
func (cm *CollectionManager) DeleteCollection(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, exists := cm.collections[name]; exists {
		delete(cm.collections, name)
		slog.Info("Collection deleted from memory", "name", name)
	} else {
		slog.Warn("Attempted to delete non-existent collection", "name", name)
	}
}

// ListCollections returns the names of all active collections.
func (cm *CollectionManager) ListCollections() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	names := make([]string, 0, len(cm.collections))
	for name := range cm.collections {
		names = append(names, name)
	}
	slog.Debug("Listing collections", "count", len(names))
	return names
}

// CollectionExists checks if a collection with the given name exists in the manager.
func (cm *CollectionManager) CollectionExists(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, exists := cm.collections[name]
	return exists
}

// GetAllCollectionsDataForPersistence gets data from all managed InMemStore instances for persistence.
func (cm *CollectionManager) GetAllCollectionsDataForPersistence() map[string]map[string][]byte {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	dataToSave := make(map[string]map[string][]byte)
	for colName, col := range cm.collections {
		dataToSave[colName] = col.GetAll()
	}
	slog.Debug("Retrieved all collections data for persistence", "collection_count", len(dataToSave))
	return dataToSave
}

// CleanExpiredItemsAndSave triggers TTL cleanup on all managed collections.
func (cm *CollectionManager) CleanExpiredItemsAndSave() {
	cm.mu.RLock()
	collectionsAndNames := make(map[string]DataStore, len(cm.collections))
	maps.Copy(collectionsAndNames, cm.collections)
	cm.mu.RUnlock()

	slog.Info("Starting TTL sweep across all collections")
	for name, col := range collectionsAndNames {
		if col.CleanExpiredItems() {
			cm.EnqueueSaveTask(name, col)
		}
	}
	slog.Info("Finished TTL sweep across all collections")
}

// EvictColdData iterates over all collections and removes "cold" data from RAM.
func (cm *CollectionManager) EvictColdData(threshold time.Time) {
	cm.mu.RLock()
	collectionsToClean := make(map[string]DataStore, len(cm.collections))
	maps.Copy(collectionsToClean, cm.collections)
	cm.mu.RUnlock()

	for name, col := range collectionsToClean {
		if inMemStore, ok := col.(*InMemStore); ok {
			inMemStore.EvictColdData(name, threshold)
		}
	}
}

// EvictColdData iterates through all shards and removes items that have become "cold".
func (s *InMemStore) EvictColdData(collectionName string, threshold time.Time) {
	totalEvicted := 0
	indexedFields := s.indexes.ListIndexes() // Extraemos los campos indexados

	for i, shard := range s.shards {
		shard.mu.Lock()
		evictedInShard := 0
		for key, item := range shard.data {
			rawBson := bson.Raw(item.Value)
			createdAtVal, err := rawBson.LookupErr(globalconst.CREATED_AT)

			if err != nil || createdAtVal.Type != bson.TypeDateTime {
				continue
			}

			createdAt := createdAtVal.Time()

			if createdAt.Before(threshold) {
				data := extractIndexedValues(item.Value, indexedFields) // Zero-Copy
				if data != nil {
					s.indexes.Remove(key, data)
				}

				delete(shard.data, key)
				evictedInShard++
			}
		}
		shard.mu.Unlock()
		if evictedInShard > 0 {
			totalEvicted += evictedInShard
			slog.Debug("Evicted cold items from shard", "collection", collectionName, "shard_id", i, "count", evictedInShard)
		}
	}
	if totalEvicted > 0 {
		slog.Info("Finished evicting cold data from collection", "collection", collectionName, "total_evicted", totalEvicted)
	}
}

func (cm *CollectionManager) GetFileLock(collectionName string) *sync.Mutex {
	cm.fileLocksMu.RLock()
	lock, exists := cm.fileLocks[collectionName]
	cm.fileLocksMu.RUnlock()

	if exists {
		return lock
	}

	cm.fileLocksMu.Lock()
	defer cm.fileLocksMu.Unlock()
	lock, exists = cm.fileLocks[collectionName]
	if exists {
		return lock
	}

	newLock := &sync.Mutex{}
	cm.fileLocks[collectionName] = newLock
	return newLock
}

// lockKeys attempts to acquire a lock for a set of keys within this shard.
func (s *Shard) lockKeys(txID string, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		if ownerTxID, isLocked := s.keyLocks[key]; isLocked && ownerTxID != txID {
			return fmt.Errorf("key '%s' is already locked by transaction '%s'", key, ownerTxID)
		}
	}

	for _, key := range keys {
		s.keyLocks[key] = txID
	}

	return nil
}

// prepareWrite stores changes in the "pendingWrites" area.
func (s *Shard) prepareWrite(txID string, op WriteOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if owner, ok := s.keyLocks[op.Key]; !ok || owner != txID {
		return fmt.Errorf("cannot prepare write for key '%s': not locked by transaction '%s'", op.Key, txID)
	}

	if _, exists := s.pendingWrites[txID]; !exists {
		s.pendingWrites[txID] = make(map[string]Item)
	}

	var pendingItem Item
	if op.OpType == OpTypeDelete {
		pendingItem = Item{Value: nil}
	} else {
		createdAt := time.Now()
		if existingItem, exists := s.data[op.Key]; exists {
			createdAt = existingItem.CreatedAt
		}
		pendingItem = Item{
			Value:     op.Value,
			CreatedAt: createdAt,
			TTL:       0,
		}
	}

	s.pendingWrites[txID][op.Key] = pendingItem
	return nil
}

// commitAppliedChanges applies pendingWrites changes to the main data store.
func (s *Shard) commitAppliedChanges(txID string, indexManager *IndexManager) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pendingOps, ok := s.pendingWrites[txID]
	if !ok {
		for key, owner := range s.keyLocks {
			if owner == txID {
				delete(s.keyLocks, key)
			}
		}
		return
	}

	// Conseguimos la lista de índices solo una vez
	var indexedFields []string
	if indexManager != nil {
		indexedFields = indexManager.ListIndexes()
	}

	for key, newItem := range pendingOps {
		var oldDataForIndex map[string]any
		if oldItem, exists := s.data[key]; exists && oldItem.Value != nil {
			if indexManager != nil {
				oldDataForIndex = extractIndexedValues(oldItem.Value, indexedFields) // Zero-Copy
			}
		}

		if newItem.Value == nil {
			delete(s.data, key)
			if oldDataForIndex != nil && indexManager != nil {
				indexManager.Remove(key, oldDataForIndex)
			}
		} else {
			s.data[key] = newItem
			if indexManager != nil {
				newDataForIndex := extractIndexedValues(newItem.Value, indexedFields) // Zero-Copy
				indexManager.Update(key, oldDataForIndex, newDataForIndex)
			}
		}

		delete(s.keyLocks, key)
	}

	delete(s.pendingWrites, txID)
}

// rollbackChanges discards pending changes and releases locks.
func (s *Shard) rollbackChanges(txID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if pendingOps, ok := s.pendingWrites[txID]; ok {
		for key := range pendingOps {
			delete(s.keyLocks, key)
		}
		delete(s.pendingWrites, txID)
	} else {
		for key, owner := range s.keyLocks {
			if owner == txID {
				delete(s.keyLocks, key)
			}
		}
	}
}

// StreamAll iterates through all non-expired items across all shards and executes a callback.
func (s *InMemStore) StreamAll(callback func(key string, value []byte) bool) {
	now := time.Now()

	for _, shard := range s.shards {
		keepGoing := true
		shard.mu.RLock()
		for k, item := range shard.data {
			if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
				if !callback(k, item.Value) {
					keepGoing = false
					break
				}
			}
		}
		shard.mu.RUnlock()
		if !keepGoing {
			break
		}
	}
}

// StreamByIndex recorre un índice B-Tree en orden (ascendente o descendente).
func (im *IndexManager) StreamByIndex(field string, descending bool, callback func(key string) bool) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return false
	}

	keepGoing := true

	if descending {
		// Descendente: Textos Z-A, luego Números Mayor-Menor
		index.stringTree.Descend(func(item StringKey) bool {
			for k := range item.Keys {
				if !callback(k) {
					keepGoing = false
					return false
				}
			}
			return true
		})
		if !keepGoing {
			return true
		}
		index.numericTree.Descend(func(item NumericKey) bool {
			for k := range item.Keys {
				if !callback(k) {
					keepGoing = false
					return false
				}
			}
			return true
		})
	} else {
		// Ascendente: Números Menor-Mayor, luego Textos A-Z
		index.numericTree.Ascend(func(item NumericKey) bool {
			for k := range item.Keys {
				if !callback(k) {
					keepGoing = false
					return false
				}
			}
			return true
		})
		if !keepGoing {
			return true
		}
		index.stringTree.Ascend(func(item StringKey) bool {
			for k := range item.Keys {
				if !callback(k) {
					keepGoing = false
					return false
				}
			}
			return true
		})
	}
	return true
}

// StreamByIndex expone el recorrido del B-Tree desde el InMemStore.
func (s *InMemStore) StreamByIndex(field string, descending bool, callback func(key string) bool) bool {
	return s.indexes.StreamByIndex(field, descending, callback)
}

// Update reemplaza un valor existente optimizando el uso de bloqueos.
func (s *InMemStore) Update(key string, newValue []byte) bool {
	// 1. TRABAJO PESADO AFUERA: Calculamos la nueva caché de índices SIN bloquear el Shard.
	indexedFields := s.indexes.ListIndexes()
	newDataForIndex := extractIndexedValues(newValue, indexedFields)

	shard := s.getShard(key)
	shard.mu.Lock() // <-- INICIA ZONA CRÍTICA

	if _, isLocked := shard.keyLocks[key]; isLocked {
		shard.mu.Unlock()
		return false
	}

	oldItem, exists := shard.data[key]
	if !exists {
		shard.mu.Unlock()
		return false
	}

	oldDataForIndex := oldItem.IndexedValues

	newItem := Item{
		Value:         newValue,
		IndexedValues: newDataForIndex, // Usamos la caché precalculada
		CreatedAt:     oldItem.CreatedAt,
		TTL:           oldItem.TTL,
	}

	shard.data[key] = newItem
	shard.mu.Unlock() // <-- FIN DE ZONA CRÍTICA (¡Intercambio en nanosegundos!)

	// 3. Actualizamos los B-Trees de forma asíncrona al Shard
	s.indexes.Update(key, oldDataForIndex, newDataForIndex)

	return true
}

// =========================================================================
// 🚀 OPTIMIZACIONES EXTREMAS: INDEX-ONLY SCANS
// =========================================================================

// GetDistinctValues extrae los valores únicos directamente recorriendo el B-Tree en O(K).
func (im *IndexManager) GetDistinctValues(field string) ([]any, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false
	}

	var results []any
	// Recorremos los nodos del árbol numérico
	index.numericTree.Ascend(func(item NumericKey) bool {
		results = append(results, item.Value)
		return true
	})
	// Recorremos los nodos del árbol de textos
	index.stringTree.Ascend(func(item StringKey) bool {
		results = append(results, item.Value)
		return true
	})
	return results, true
}

// GetGroupedCount realiza un GROUP BY + COUNT(*) instantáneo sumando la longitud de los arrays de IDs.
func (im *IndexManager) GetGroupedCount(field string) (map[any]int, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false
	}

	results := make(map[any]int)
	index.numericTree.Ascend(func(item NumericKey) bool {
		results[item.Value] = len(item.Keys) // Magia: Len() es O(1)
		return true
	})
	index.stringTree.Ascend(func(item StringKey) bool {
		results[item.Value] = len(item.Keys) // Magia: Len() es O(1)
		return true
	})
	return results, true
}

// Implementación en el InMemStore (Delega al IndexManager)
func (s *InMemStore) GetDistinctValues(field string) ([]any, bool) {
	return s.indexes.GetDistinctValues(field)
}

func (s *InMemStore) GetGroupedCount(field string) (map[any]int, bool) {
	return s.indexes.GetGroupedCount(field)
}
