package store

import (
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

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
				result[field] = int64(val.Int32())
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

// IndexManager ahora solo actúa como registro de metadatos (Zero RAM data).
type IndexManager struct {
	mu      sync.RWMutex
	indexes map[string]struct{}
}

func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]struct{}),
	}
}

func (im *IndexManager) CreateIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.indexes[field] = struct{}{}
}

func (im *IndexManager) DeleteIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	delete(im.indexes, field)
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

func (im *IndexManager) HasIndex(field string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, exists := im.indexes[field]
	return exists
}
