package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"lunadb/internal/globalconst"
	"os"
	"path/filepath"
	"time"

	"github.com/edsrzf/mmap-go"
	"go.mongodb.org/mongo-driver/bson"
)

// rewriteCollectionFile atomically rewrites a collection's data file.
// Keeps using Standard I/O as it involves writing to a new temp file.
func rewriteCollectionFile(collectionName string, updateFunc func(key string, data []byte) ([]byte, error)) error {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	tempFilePath := filePath + ".tmp"

	sourceFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist, nothing to rewrite.
		}
		return fmt.Errorf("failed to open source collection file '%s': %w", filePath, err)
	}
	defer sourceFile.Close()

	destFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary collection file '%s': %w", tempFilePath, err)
	}
	defer destFile.Close()

	// Preserve the index header.
	var numIndexes uint32
	if err := binary.Read(sourceFile, binary.LittleEndian, &numIndexes); err != nil {
		return fmt.Errorf("rewrite: failed to read index header count: %w", err)
	}
	if err := binary.Write(destFile, binary.LittleEndian, numIndexes); err != nil {
		return fmt.Errorf("rewrite: failed to write index header count: %w", err)
	}

	indexedFields := make([][]byte, numIndexes)
	for i := 0; i < int(numIndexes); i++ {
		fieldBytes, err := readPrefixedBytes(sourceFile)
		if err != nil {
			return fmt.Errorf("rewrite: failed to read index field name: %w", err)
		}
		indexedFields[i] = fieldBytes
		if err := writePrefixedBytes(destFile, fieldBytes); err != nil {
			return fmt.Errorf("rewrite: failed to write index field name: %w", err)
		}
	}

	var numEntries uint32
	if err := binary.Read(sourceFile, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("rewrite: failed to read entry count: %w", err)
	}

	if err := binary.Write(destFile, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("rewrite: failed to write placeholder entry count: %w", err)
	}

	var finalCount uint32
	for i := 0; i < int(numEntries); i++ {
		keyBytes, err := readPrefixedBytes(sourceFile)
		if err != nil {
			return fmt.Errorf("rewrite: failed to read key at entry %d: %w", i, err)
		}
		valBytes, err := readPrefixedBytes(sourceFile)
		if err != nil {
			return fmt.Errorf("rewrite: failed to read value at entry %d: %w", i, err)
		}

		newValBytes, err := updateFunc(string(keyBytes), valBytes)
		if err != nil {
			return fmt.Errorf("rewrite: update function failed for key '%s': %w", string(keyBytes), err)
		}

		if newValBytes != nil {
			if err := writePrefixedBytes(destFile, keyBytes); err != nil {
				return fmt.Errorf("rewrite: failed to write key for '%s': %w", string(keyBytes), err)
			}
			if err := writePrefixedBytes(destFile, newValBytes); err != nil {
				return fmt.Errorf("rewrite: failed to write value for '%s': %w", string(keyBytes), err)
			}
			finalCount++
		}
	}

	// Go back to the beginning to write the final count.
	if _, err := destFile.Seek(0, 0); err != nil {
		return fmt.Errorf("rewrite: failed to seek to start of temp file: %w", err)
	}

	// Re-write the header (indexes and final count).
	if err := binary.Write(destFile, binary.LittleEndian, numIndexes); err != nil {
		return fmt.Errorf("rewrite: failed to write final index count: %w", err)
	}
	for _, fieldBytes := range indexedFields {
		if err := writePrefixedBytes(destFile, fieldBytes); err != nil {
			return fmt.Errorf("rewrite: failed to write final index field name: %w", err)
		}
	}
	if err := binary.Write(destFile, binary.LittleEndian, finalCount); err != nil {
		return fmt.Errorf("rewrite: failed to write final entry count: %w", err)
	}

	if err := destFile.Close(); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("rewrite: failed to close temp file: %w", err)
	}
	if err := os.Rename(tempFilePath, filePath); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("rewrite: failed to rename temp file: %w", err)
	}

	return nil
}

// UpdateColdItem finds a cold item by key and applies a patch to it on disk.
func UpdateColdItem(collectionName, key string, patchValue []byte) (bool, error) {
	found := false
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		if itemKey != key {
			return data, nil
		}

		found = true
		var existingData bson.M
		if err := bson.Unmarshal(data, &existingData); err != nil {
			return nil, fmt.Errorf("could not unmarshal existing cold data: %w", err)
		}

		var patchData bson.M
		if err := bson.Unmarshal(patchValue, &patchData); err != nil {
			return nil, fmt.Errorf("could not unmarshal patch data: %w", err)
		}

		for k, v := range patchData {
			if k == globalconst.ID || k == globalconst.CREATED_AT {
				continue
			}
			existingData[k] = v
		}
		existingData[globalconst.UPDATED_AT] = time.Now().UTC()

		return bson.Marshal(existingData)
	})

	return found, err
}

// DeleteColdItem finds a cold item by key and marks it as deleted on disk (tombstone).
func DeleteColdItem(collectionName, key string) (bool, error) {
	found := false
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		if itemKey != key {
			return data, nil
		}

		found = true
		var doc bson.M
		if err := bson.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("could not unmarshal cold data for deletion: %w", err)
		}

		doc[globalconst.DELETED_FLAG] = true
		doc[globalconst.UPDATED_AT] = time.Now().UTC()

		return bson.Marshal(doc)
	})

	return found, err
}

// CompactCollectionFile rewrites a collection file, permanently removing tombstones.
func CompactCollectionFile(collectionName string) error {
	slog.Info("Compacting collection file", "collection", collectionName)
	return rewriteCollectionFile(collectionName, func(key string, data []byte) ([]byte, error) {
		var doc bson.M
		if err := bson.Unmarshal(data, &doc); err != nil {
			return data, nil
		}

		if deleted, ok := doc[globalconst.DELETED_FLAG].(bool); ok && deleted {
			return nil, nil // Return nil to permanently delete the record.
		}

		return data, nil // Keep this record.
	})
}

// writePrefixedBytes is a helper for the rewriter.
func writePrefixedBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// readPrefixedBytes is a helper function to read length-prefixed data (Required by rewriteCollectionFile).
func readPrefixedBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("could not read length prefix: %w", err)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("could not read full data bytes: %w", err)
	}
	return data, nil
}

// ColdUpdatePayload defines the structure for a single update operation in a batch.
type ColdUpdatePayload struct {
	ID    string
	Patch map[string]any
}

// UpdateManyColdItems updates multiple cold items in a single file rewrite operation.
func UpdateManyColdItems(collectionName string, payloads []ColdUpdatePayload) (int, error) {
	patches := make(map[string]bson.M, len(payloads))
	for _, p := range payloads {
		if p.ID != "" {
			patches[p.ID] = p.Patch
		}
	}

	updatedCount := 0
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		if patchData, ok := patches[itemKey]; ok {
			updatedCount++
			var existingData bson.M
			if err := bson.Unmarshal(data, &existingData); err != nil {
				return nil, fmt.Errorf("could not unmarshal existing cold data for batch update: %w", err)
			}

			for k, v := range patchData {
				if k == globalconst.ID || k == globalconst.CREATED_AT {
					continue
				}
				existingData[k] = v
			}
			existingData[globalconst.UPDATED_AT] = time.Now().UTC()

			return bson.Marshal(existingData)
		}

		return data, nil
	})

	return updatedCount, err
}

// DeleteManyColdItems marks multiple cold items as deleted (tombstone) in a single file rewrite.
func DeleteManyColdItems(collectionName string, keys []string) (int, error) {
	keysToDelete := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keysToDelete[k] = struct{}{}
	}

	markedCount := 0
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		if _, shouldDelete := keysToDelete[itemKey]; shouldDelete {
			markedCount++
			var doc bson.M
			if err := bson.Unmarshal(data, &doc); err != nil {
				return nil, fmt.Errorf("could not unmarshal cold data for batch deletion: %w", err)
			}

			doc[globalconst.DELETED_FLAG] = true
			doc[globalconst.UPDATED_AT] = time.Now().UTC()

			return bson.Marshal(doc)
		}

		return data, nil
	})

	return markedCount, err
}

// CheckColdKeyExists checks if a specific key exists in a collection's persistence file.
// OPTIMIZATION: Uses mmap for ultra-fast existence checks (Cross-Platform).
func CheckColdKeyExists(collectionName, keyToFind string) (bool, error) {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to open cold data file '%s': %w", filePath, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return false, nil
	}
	if stat.Size() == 0 {
		return false, nil
	}

	// MMAP Cross-Platform
	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return false, fmt.Errorf("failed to mmap file: %w", err)
	}
	defer data.Unmap()

	offset := 0
	readUint32 := func() (uint32, bool) {
		if offset+4 > len(data) {
			return 0, false
		}
		val := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		return val, true
	}

	// Skip Indexes
	numIndexes, ok := readUint32()
	if !ok {
		return false, nil
	}
	for i := 0; i < int(numIndexes); i++ {
		fieldLen, ok := readUint32()
		if !ok {
			return false, nil
		}
		offset += int(fieldLen)
		if offset > len(data) {
			return false, nil
		}
	}

	// Loop Entries
	numEntries, ok := readUint32()
	if !ok {
		return false, nil
	}

	for i := 0; i < int(numEntries); i++ {
		keyLen, ok := readUint32()
		if !ok {
			break
		}
		keyEnd := offset + int(keyLen)
		if keyEnd > len(data) {
			break
		}
		keyBytes := data[offset:keyEnd]
		offset = keyEnd

		if string(keyBytes) == keyToFind {
			return true, nil
		}

		valLen, ok := readUint32()
		if !ok {
			break
		}
		offset += int(valLen)
		if offset > len(data) {
			break
		}
	}

	return false, nil
}

// CheckManyColdKeysExist verifies the existence of multiple keys in a collection's file in a single pass.
// OPTIMIZATION: Uses mmap for high-performance batch checks (Cross-Platform).
func CheckManyColdKeysExist(collectionName string, keysToFind []string) (map[string]bool, error) {
	foundKeys := make(map[string]bool)
	if len(keysToFind) == 0 {
		return foundKeys, nil
	}

	keysMap := make(map[string]struct{}, len(keysToFind))
	for _, k := range keysToFind {
		keysMap[k] = struct{}{}
	}

	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return foundKeys, nil
		}
		return nil, fmt.Errorf("failed to open cold data file '%s': %w", filePath, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return foundKeys, nil
	}
	if stat.Size() == 0 {
		return foundKeys, nil
	}

	// MMAP Cross-Platform
	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}
	defer data.Unmap()

	offset := 0
	readUint32 := func() (uint32, bool) {
		if offset+4 > len(data) {
			return 0, false
		}
		val := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		return val, true
	}

	// Skip Indexes
	numIndexes, ok := readUint32()
	if !ok {
		return foundKeys, nil
	}
	for i := 0; i < int(numIndexes); i++ {
		fieldLen, ok := readUint32()
		if !ok {
			return foundKeys, nil
		}
		offset += int(fieldLen)
		if offset > len(data) {
			return foundKeys, nil
		}
	}

	// Loop Entries
	numEntries, ok := readUint32()
	if !ok {
		return foundKeys, nil
	}

	for i := 0; i < int(numEntries); i++ {
		keyLen, ok := readUint32()
		if !ok {
			break
		}
		keyEnd := offset + int(keyLen)
		if keyEnd > len(data) {
			break
		}
		keyBytes := data[offset:keyEnd]
		offset = keyEnd

		if _, needed := keysMap[string(keyBytes)]; needed {
			foundKeys[string(keyBytes)] = true
		}

		valLen, ok := readUint32()
		if !ok {
			break
		}
		offset += int(valLen)
		if offset > len(data) {
			break
		}

		if len(foundKeys) == len(keysToFind) {
			break
		}
	}

	return foundKeys, nil
}
