/* ==========================================================
   Ruta y Archivo: ./internal/persistence/cold_search.go
   ========================================================== */

package persistence

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"lunadb/internal/globalconst"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
	"go.mongodb.org/mongo-driver/bson"
)

// MatcherFunc is a function signature that defines how to determine if a document matches a filter.
type MatcherFunc func(itemBSON []byte) bool

// SearchColdData searches a collection's persistence file for items that match a filter.
func SearchColdData(collectionName string, matcher MatcherFunc) ([]bson.M, error) {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []bson.M{}, nil
		}
		return nil, fmt.Errorf("failed to open cold data file '%s': %w", filePath, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	size := stat.Size()
	if size == 0 {
		return []bson.M{}, nil
	}

	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}
	defer func() {
		if err := data.Unmap(); err != nil {
			slog.Error("Failed to unmap file", "error", err)
		}
	}()

	offset := 0
	readUint32 := func() (uint32, bool) {
		if offset+4 > len(data) {
			return 0, false
		}
		val := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		return val, true
	}

	// 1. Skip Index Header
	numIndexes, ok := readUint32()
	if !ok {
		return nil, fmt.Errorf("corrupted file: unexpected EOF reading index count")
	}

	for i := 0; i < int(numIndexes); i++ {
		fieldLen, ok := readUint32()
		if !ok {
			return nil, fmt.Errorf("corrupted file: unexpected EOF reading index len")
		}
		offset += int(fieldLen)
	}

	// 2. Read Entry Count
	numEntries, ok := readUint32()
	if !ok {
		return []bson.M{}, nil
	}

	results := make([]bson.M, 0)

	for i := 0; i < int(numEntries); i++ {
		keyLen, ok := readUint32()
		if !ok {
			break
		}
		offset += int(keyLen)
		if offset > len(data) {
			break
		}

		valLen, ok := readUint32()
		if !ok {
			break
		}
		valEnd := offset + int(valLen)
		if valEnd > len(data) {
			break
		}

		valBytes := data[offset:valEnd]
		offset = valEnd

		// 3. ZERO-COPY BSON LOOKUP: Revisamos si está borrado sin deserializar
		rawBson := bson.Raw(valBytes)
		if deletedVal, err := rawBson.LookupErr(globalconst.DELETED_FLAG); err == nil {
			if deletedVal.Boolean() {
				continue // Saltamos tombstones muy rápido
			}
		}

		// 4. Ejecución del filtro
		if matcher(valBytes) {
			var doc bson.M
			if err := bson.Unmarshal(valBytes, &doc); err == nil {
				results = append(results, doc)
			}
		}
	}

	slog.Debug("Cold data mmap search complete", "collection", collectionName, "found_matches", len(results))
	return results, nil
}
