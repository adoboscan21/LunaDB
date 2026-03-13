/* ==========================================================
   Ruta y Archivo: ./internal/store/store.go
   ========================================================== */

package store

import (
	"time"
)

// ItemRecord es la estructura que serializaremos en el disco (bbolt).
// Usamos tags cortos de BSON para ahorrar espacio en disco.
type ItemRecord struct {
	Value     []byte        `bson:"v"`
	CreatedAt time.Time     `bson:"c"`
	TTL       time.Duration `bson:"t"`
}

type DataStore interface {
	Set(key string, value []byte, ttl time.Duration)
	SetMany(items map[string][]byte) // NUEVO
	Get(key string) ([]byte, bool)
	GetMany(keys []string) map[string][]byte
	Delete(key string)
	DeleteMany(keys []string) // NUEVO
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
	UpdateMany(patches map[string][]byte) (int, []string)
	GetDistinctValues(field string) ([]any, bool)
	GetGroupedCount(field string) (map[any]int, bool)
}
