/* ==========================================================
   Ruta y Archivo: ./internal/store/index.go
   ========================================================== */

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
	Keys  []string // OPTIMIZADO: Slice en lugar de map para iteración ultra rápida
}

// StringKey implements the item for the string B-Tree.
type StringKey struct {
	Value string
	Keys  []string // OPTIMIZADO: Slice en lugar de map para iteración ultra rápida
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

// IndexManager manages all indexes for a single Store.
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
			item = NumericKey{Value: fVal, Keys: make([]string, 0, 1)}
		}
		item.Keys = append(item.Keys, docKey)
		index.numericTree.ReplaceOrInsert(item)
	} else if sVal, ok := value.(string); ok {
		key := StringKey{Value: sVal}
		item, found := index.stringTree.Get(key)
		if !found {
			item = StringKey{Value: sVal, Keys: make([]string, 0, 1)}
		}
		item.Keys = append(item.Keys, docKey)
		index.stringTree.ReplaceOrInsert(item)
	}
}

// removeFromIndex implements Lazy Delete for O(1) performance during bulk operations.
func (im *IndexManager) removeFromIndex(index *Index, docKey string, value any) {
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

	var foundKeys []string
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

	// Devolvemos una copia del slice para evitar mutaciones externas que corrompan el B-Tree
	keys := make([]string, len(foundKeys))
	copy(keys, foundKeys)
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
			for _, k := range item.Keys {
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
				for _, k := range item.Keys {
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
			for _, k := range item.Keys {
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
				for _, k := range item.Keys {
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
		index.stringTree.Descend(func(item StringKey) bool {
			for _, k := range item.Keys {
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
			for _, k := range item.Keys {
				if !callback(k) {
					keepGoing = false
					return false
				}
			}
			return true
		})
	} else {
		index.numericTree.Ascend(func(item NumericKey) bool {
			for _, k := range item.Keys {
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
			for _, k := range item.Keys {
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

// GetDistinctValues extrae los valores únicos directamente recorriendo el B-Tree.
func (im *IndexManager) GetDistinctValues(field string) ([]any, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false
	}

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
		results[item.Value] = len(item.Keys)
		return true
	})
	index.stringTree.Ascend(func(item StringKey) bool {
		results[item.Value] = len(item.Keys)
		return true
	})
	return results, true
}
