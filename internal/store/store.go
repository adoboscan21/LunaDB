package store

import (
	"bytes"
	"compress/zlib"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// ItemRecord es la estructura que serializaremos en el disco (bbolt).
// Usamos tags cortos de BSON para ahorrar espacio en disco.
type ItemRecord struct {
	Value           []byte    `bson:"v"`
	CreatedAt       time.Time `bson:"c"`
	Version         uint64    `bson:"ver"`
	CompressionType byte      `bson:"comp,omitempty"` // 0: no compresión, 1: zlib
}

func compressBytes(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decompressBytes(data []byte) ([]byte, error) {
	b := bytes.NewReader(data)
	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out bytes.Buffer
	if _, err := io.Copy(&out, r); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// MarshalRecord serializa un ItemRecord, comprimiendo el valor si supera los 512 bytes.
func MarshalRecord(rec ItemRecord) ([]byte, error) {
	if len(rec.Value) > 512 {
		compressed, err := compressBytes(rec.Value)
		if err == nil {
			rec.Value = compressed
			rec.CompressionType = 1
		}
	}
	return bson.Marshal(rec)
}

// UnmarshalRecord deserializa los bytes a un ItemRecord y los descomprime si CompressionType == 1.
func UnmarshalRecord(data []byte, rec *ItemRecord) error {
	if err := bson.Unmarshal(data, rec); err != nil {
		return err
	}
	if rec.CompressionType == 1 {
		decompressed, err := decompressBytes(rec.Value)
		if err != nil {
			return err
		}
		rec.Value = decompressed
	}
	return nil
}


type DataStore interface {
	Set(key string, value []byte)
	SetMany(items map[string][]byte) (int, int)
	Get(key string) ([]byte, bool)
	GetMany(keys []string) map[string][]byte
	Delete(key string)
	DeleteMany(keys []string)
	GetAll() map[string][]byte
	StreamAll(callback func(key string, value []byte) bool)
	LoadData(data map[string][]byte)
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
	IndexCount(field string, value any) int
	IndexRangeCount(field string, low, high any, lowInclusive, highInclusive bool) int
}
