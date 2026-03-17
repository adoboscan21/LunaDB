package store

import (
	"log/slog"
	"strconv"
	"strings"
	"sync"

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
	Keys  map[string]struct{} // SOLUCIÓN: Búsqueda y borrado en O(1) verdadero
}

// StringKey implements the item for the string B-Tree.
type StringKey struct {
	Value string
	Keys  map[string]struct{} // SOLUCIÓN: Búsqueda y borrado en O(1) verdadero
}

func numericLess(a, b NumericKey) bool { return a.Value < b.Value }
func stringLess(a, b StringKey) bool   { return a.Value < b.Value }

type Index struct {
	mu          sync.RWMutex
	numericTree *btree.BTreeG[NumericKey]
	stringTree  *btree.BTreeG[StringKey]
}

func NewIndex() *Index {
	return &Index{
		numericTree: btree.NewG[NumericKey](btreeDegree, numericLess),
		stringTree:  btree.NewG[StringKey](btreeDegree, stringLess),
	}
}

type IndexManager struct {
	mu      sync.RWMutex
	indexes map[string]*Index
}

func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
	}
}

func (im *IndexManager) CreateIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; !exists {
		im.indexes[field] = NewIndex()
		slog.Info("B-Tree Index created", "field", field)
	}
}

func (im *IndexManager) DeleteIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; exists {
		delete(im.indexes, field)
		slog.Info("Index deleted", "field", field)
	}
}

func (im *IndexManager) ListIndexes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()
	indexedFields := make([]string, 0, len(im.indexes))
	for field := range im.indexes {
		indexedFields = append(indexedFields, field)
	}
	return indexedFields
}

func (im *IndexManager) addToIndex(index *Index, docKey string, value any) {
	index.mu.Lock()
	defer index.mu.Unlock()

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

func (im *IndexManager) removeFromIndex(index *Index, docKey string, value any) {
	index.mu.Lock()
	defer index.mu.Unlock()

	if fVal, ok := valueToFloat64(value); ok {
		key := NumericKey{Value: fVal}
		if item, found := index.numericTree.Get(key); found {
			delete(item.Keys, docKey) // O(1) instantáneo
			if len(item.Keys) == 0 {
				index.numericTree.Delete(key)
			}
		}
	} else if sVal, ok := value.(string); ok {
		key := StringKey{Value: sVal}
		if item, found := index.stringTree.Get(key); found {
			delete(item.Keys, docKey) // O(1) instantáneo
			if len(item.Keys) == 0 {
				index.stringTree.Delete(key)
			}
		}
	}
}

func (im *IndexManager) Update(docKey string, oldData, newData map[string]any) {
	im.mu.RLock()
	activeIndexes := make(map[string]*Index, len(im.indexes))
	for field, idx := range im.indexes {
		activeIndexes[field] = idx
	}
	im.mu.RUnlock()

	if len(activeIndexes) == 0 {
		return
	}

	for field, index := range activeIndexes {
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

func (im *IndexManager) Remove(docKey string, data map[string]any) {
	im.mu.RLock()
	activeIndexes := make(map[string]*Index, len(im.indexes))
	for field, idx := range im.indexes {
		activeIndexes[field] = idx
	}
	im.mu.RUnlock()

	if data == nil || len(activeIndexes) == 0 {
		return
	}
	for field, index := range activeIndexes {
		if val, ok := data[field]; ok {
			im.removeFromIndex(index, docKey, val)
		}
	}
}

func (im *IndexManager) Lookup(field string, value any) ([]string, bool) {
	im.mu.RLock()
	index, exists := im.indexes[field]
	im.mu.RUnlock()

	if !exists {
		return nil, false
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

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

func (im *IndexManager) LookupRange(field string, low, high any, lowInclusive, highInclusive bool) ([]string, bool) {
	im.mu.RLock()
	index, exists := im.indexes[field]
	im.mu.RUnlock()

	if !exists {
		return nil, false
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

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

func (im *IndexManager) HasIndex(field string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, exists := im.indexes[field]
	return exists
}

func (im *IndexManager) StreamByIndex(field string, descending bool, callback func(key string) bool) bool {
	im.mu.RLock()
	index, exists := im.indexes[field]
	im.mu.RUnlock()

	if !exists {
		return false
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

	keepGoing := true

	if descending {
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

func (im *IndexManager) GetDistinctValues(field string) ([]any, bool) {
	im.mu.RLock()
	index, exists := im.indexes[field]
	im.mu.RUnlock()

	if !exists {
		return nil, false
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

	var results []any
	index.numericTree.Ascend(func(item NumericKey) bool {
		results = append(results, item.Value)
		return true
	})
	index.stringTree.Ascend(func(item StringKey) bool {
		results = append(results, item.Value)
		return true
	})
	return results, true
}

func (im *IndexManager) GetGroupedCount(field string) (map[any]int, bool) {
	im.mu.RLock()
	index, exists := im.indexes[field]
	im.mu.RUnlock()

	if !exists {
		return nil, false
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

	results := make(map[any]int)
	index.numericTree.Ascend(func(item NumericKey) bool {
		results[item.Value] = len(item.Keys)
		return true
	})
	index.stringTree.Ascend(func(item StringKey) bool {
		results[item.Value] = len(item.Keys)
		return true
	})
	return results, true
}
