package handler

import (
	"fmt"
	"io"
	"log/slog"
	"time"

	"lunadb/internal/globalconst"
	"lunadb/internal/protocol"
	"lunadb/internal/store"
	"net"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

// HandleCollectionItemSet processes the CmdCollectionItemSet command.
func (h *ConnectionHandler) HandleCollectionItemSet(r io.Reader, conn net.Conn) {
	collectionName, key, value, err := protocol.ReadCollectionItemSetCommand(r)
	if err != nil {
		slog.Error("Failed to read COLLECTION_ITEM_SET command payload", "error", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	// Auto-generación de llave si viene vacía
	if key == "" {
		const maxGenerateRetries = 5
		for i := 0; i < maxGenerateRetries; i++ {
			newKey := uuid.New().String()
			if _, found := colStore.Get(newKey); !found {
				key = newKey
				break
			}
		}
		if key == "" {
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to generate a unique ID", nil)
			return
		}
	} else if h.CurrentTransactionID == "" {
		// Validar unicidad si no es transacción
		if _, found := colStore.Get(key); found {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Key '%s' already exists in collection '%s'. Use 'update'.", key, collectionName), nil)
			return
		}
	}

	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: No write permission for '%s'", collectionName), nil)
		return
	}

	var data bson.M
	if err := bson.Unmarshal(value, &data); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid value. Must be a BSON object.", nil)
		return
	}
	data[globalconst.ID] = key

	// Lógica Transaccional
	if h.CurrentTransactionID != "" {
		finalValueForTx, _ := bson.Marshal(data)
		op := store.WriteOperation{
			Collection: collectionName,
			Key:        key,
			Value:      finalValueForTx,
			OpType:     store.OpTypeSet,
		}

		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record in transaction: "+err.Error(), nil)
			return
		}
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: Operation queued in transaction.", finalValueForTx)
		return
	}

	// Lógica Estándar (ACID directo en bbolt)
	now := time.Now().UTC()
	data[globalconst.UPDATED_AT] = now
	data[globalconst.CREATED_AT] = now

	finalValue, _ := bson.Marshal(data)
	colStore.Set(key, finalValue)

	slog.Info("Item set in collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s'", key, collectionName), finalValue)
}

// HandleCollectionItemUpdate processes the CmdCollectionItemUpdate command.
func (h *ConnectionHandler) HandleCollectionItemUpdate(r io.Reader, conn net.Conn) {
	collectionName, key, patchValue, err := protocol.ReadCollectionItemUpdateCommand(r)
	if err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_UPDATE command format", nil)
		return
	}

	if collectionName == "" || key == "" || len(patchValue) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name, key, or patch value cannot be empty", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: No write permission", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	existingValue, found := colStore.Get(key)
	if !found {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' does not exist.", key), nil)
		return
	}

	var patchData bson.M
	if err := bson.Unmarshal(patchValue, &patchData); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch BSON format.", nil)
		return
	}

	// Lógica Transaccional
	if h.CurrentTransactionID != "" {
		var existingData bson.M
		bson.Unmarshal(existingValue, &existingData)
		for k, v := range patchData {
			if k != globalconst.ID && k != globalconst.CREATED_AT {
				existingData[k] = v
			}
		}
		finalValue, _ := bson.Marshal(existingData)

		op := store.WriteOperation{
			Collection: collectionName, Key: key, Value: finalValue, OpType: store.OpTypeUpdate,
		}
		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record update in transaction: "+err.Error(), nil)
			return
		}
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: Update operation queued in transaction.", nil)
		return
	}

	// Fast Path Zero-Copy
	updatedValue, err := applyBSONPatchFast(existingValue, patchData, time.Now().UTC())
	if err != nil {
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to apply patch.", nil)
		return
	}

	colStore.Update(key, updatedValue)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' updated in '%s'", key, collectionName), updatedValue)
}

type updateManyPayload struct {
	ID    string `bson:"_id"`
	Patch bson.M `bson:"patch"`
}

// HandleCollectionItemUpdateMany processes multiple updates.
func (h *ConnectionHandler) HandleCollectionItemUpdateMany(r io.Reader, conn net.Conn) {
	collectionName, value, err := protocol.ReadCollectionItemUpdateManyCommand(r)
	if err != nil || collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}

	var payload struct {
		Array []updateManyPayload `bson:"array"`
	}
	if err := bson.Unmarshal(value, &payload); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid BSON array format", nil)
		return
	}

	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	now := time.Now().UTC()

	// 1. FASE DE LECTURA (En memoria) Y PARCHEO
	itemsToUpdate := make(map[string][]byte)
	var failedKeys []string

	for _, p := range payload.Array {
		existingValue, found := colStore.Get(p.ID)
		if !found {
			failedKeys = append(failedKeys, p.ID)
			continue
		}

		if h.CurrentTransactionID != "" {
			var existingData bson.M
			bson.Unmarshal(existingValue, &existingData)
			for k, v := range p.Patch {
				if k != globalconst.ID && k != globalconst.CREATED_AT {
					existingData[k] = v
				}
			}
			finalValue, _ := bson.Marshal(existingData)
			op := store.WriteOperation{Collection: collectionName, Key: p.ID, Value: finalValue, OpType: store.OpTypeUpdate}
			h.TransactionManager.RecordWrite(h.CurrentTransactionID, op)
			continue
		}

		// Fast Path Zero-Copy
		updatedValue, err := applyBSONPatchFast(existingValue, p.Patch, now)
		if err != nil {
			failedKeys = append(failedKeys, p.ID)
			continue
		}
		itemsToUpdate[p.ID] = updatedValue
	}

	// 2. FASE DE ESCRITURA FÍSICA (Bulk Update en disco)
	updatedCount := 0
	if h.CurrentTransactionID == "" && len(itemsToUpdate) > 0 {
		var diskFailedKeys []string
		updatedCount, diskFailedKeys = colStore.UpdateMany(itemsToUpdate)
		failedKeys = append(failedKeys, diskFailedKeys...)
	} else if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d ops queued.", len(payload.Array)-len(failedKeys)), nil)
		return
	}

	msg := fmt.Sprintf("OK: %d items updated. %d failed.", updatedCount, len(failedKeys))
	var responseData []byte
	if len(failedKeys) > 0 {
		responseData, _ = bson.Marshal(bson.M{"failed_keys": failedKeys})
	}
	protocol.WriteResponse(conn, protocol.StatusOk, msg, responseData)
}

// handleCollectionItemGet processes CmdCollectionItemGet.
func (h *ConnectionHandler) handleCollectionItemGet(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Read operations not supported inside a transaction yet.", nil)
		return
	}
	collectionName, key, err := protocol.ReadCollectionItemGetCommand(r)
	if err != nil || collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}

	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	if value, found := colStore.Get(key); found {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved", key), value)
	} else {
		protocol.WriteResponse(conn, protocol.StatusNotFound, "NOT FOUND", nil)
	}
}

// HandleCollectionItemDelete processes CmdCollectionItemDelete.
func (h *ConnectionHandler) HandleCollectionItemDelete(r io.Reader, conn net.Conn) {
	collectionName, key, err := protocol.ReadCollectionItemDeleteCommand(r)
	if err != nil || collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}

	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED", nil)
		return
	}

	if h.CurrentTransactionID != "" {
		op := store.WriteOperation{Collection: collectionName, Key: key, OpType: store.OpTypeDelete}
		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record", nil)
			return
		}
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: Delete queued in transaction.", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	if _, found := colStore.Get(key); found {
		colStore.Delete(key)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted", key), nil)
	} else {
		protocol.WriteResponse(conn, protocol.StatusNotFound, "NOT FOUND", nil)
	}
}

// handleCollectionItemList processes CmdCollectionItemList.
func (h *ConnectionHandler) handleCollectionItemList(r io.Reader, conn net.Conn) {
	collectionName, err := protocol.ReadCollectionItemListCommand(r)
	if err != nil || collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}
	if !h.IsRoot {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can list all items.", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	resultsArray := make([]bson.M, 0)

	colStore.StreamAll(func(k string, vBytes []byte) bool {
		var doc bson.M
		if err := bson.Unmarshal(vBytes, &doc); err == nil {
			if _, ok := doc[globalconst.ID]; !ok {
				doc[globalconst.ID] = k
			}
			resultsArray = append(resultsArray, doc)
		}
		return true
	})

	bsonResponseData, _ := bson.Marshal(bson.M{"results": resultsArray})
	protocol.WriteResponse(conn, protocol.StatusOk, "OK: Items retrieved", bsonResponseData)
}

// HandleCollectionItemSetMany processes CmdCollectionItemSetMany with Lazy Decoding and Bulk Ops.
func (h *ConnectionHandler) HandleCollectionItemSetMany(r io.Reader, conn net.Conn) {
	collectionName, value, err := protocol.ReadCollectionItemSetManyCommand(r)
	if err != nil || collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED", nil)
		return
	}

	rawWrapper := bson.Raw(value)
	rawArray, err := rawWrapper.LookupErr("array")
	if err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid BSON format: missing 'array'", nil)
		return
	}
	elements, _ := rawArray.Array().Elements()

	colStore := h.CollectionManager.GetCollection(collectionName)
	var processedCount, duplicateCount, invalidCount int
	now := time.Now().UTC()

	// Mapa masivo para inserción
	itemsToSet := make(map[string][]byte, len(elements))

	for _, elem := range elements {
		doc := elem.Value().Document()
		var key string
		idVal, err := doc.LookupErr(globalconst.ID)
		if err == nil && idVal.Type == bson.TypeString {
			key = idVal.StringValue()
		}

		if key == "" {
			key = uuid.New().String()
		}

		// 🔥 Usamos nuestro Fast-Path para inyectar los metadatos sin usar bson.M
		newDocBytes := enrichBSONForInsertFast(doc, key, now)

		if h.CurrentTransactionID != "" {
			op := store.WriteOperation{Collection: collectionName, Key: key, Value: newDocBytes, OpType: store.OpTypeSet}
			h.TransactionManager.RecordWrite(h.CurrentTransactionID, op)
			processedCount++
		} else {
			itemsToSet[key] = newDocBytes
		}
	}

	// Delegar inserción masiva a la capa de disco
	if h.CurrentTransactionID == "" && len(itemsToSet) > 0 {
		// 🔥 Recuperamos los contadores precisos directo del disco
		processedCount, duplicateCount = colStore.SetMany(itemsToSet)
	} else if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d set ops queued in transaction.", processedCount), nil)
		return
	}

	msg := fmt.Sprintf("OK: %d items set. %d duplicates skipped. %d invalid.", processedCount, duplicateCount, invalidCount)
	protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
}

// HandleCollectionItemDeleteMany processes CmdCollectionItemDeleteMany with Bulk Ops.
func (h *ConnectionHandler) HandleCollectionItemDeleteMany(r io.Reader, conn net.Conn) {
	collectionName, keys, err := protocol.ReadCollectionItemDeleteManyCommand(r)
	if err != nil || collectionName == "" || len(keys) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED", nil)
		return
	}

	if h.CurrentTransactionID != "" {
		for _, key := range keys {
			op := store.WriteOperation{Collection: collectionName, Key: key, OpType: store.OpTypeDelete}
			h.TransactionManager.RecordWrite(h.CurrentTransactionID, op)
		}
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d delete ops queued.", len(keys)), nil)
		return
	}

	// Delegar borrado masivo a la capa de disco
	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.DeleteMany(keys)

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys deleted.", len(keys)), nil)
}

// applyBSONPatchFast Zero-Copy BSON Patch
func applyBSONPatchFast(existingValue []byte, patchData bson.M, now time.Time) ([]byte, error) {
	rawDoc := bson.Raw(existingValue)
	elements, err := rawDoc.Elements()
	if err != nil {
		return nil, err
	}

	newDoc := make(bson.D, 0, len(elements)+len(patchData)+1)
	patchedKeys := make(map[string]bool)

	for _, elem := range elements {
		key := elem.Key()
		if key == globalconst.ID || key == globalconst.CREATED_AT {
			newDoc = append(newDoc, bson.E{Key: key, Value: elem.Value()})
			continue
		}
		if key == globalconst.UPDATED_AT {
			continue
		}

		if patchVal, exists := patchData[key]; exists {
			newDoc = append(newDoc, bson.E{Key: key, Value: patchVal})
			patchedKeys[key] = true
		} else {
			newDoc = append(newDoc, bson.E{Key: key, Value: elem.Value()})
		}
	}

	for k, v := range patchData {
		if !patchedKeys[k] && k != globalconst.ID && k != globalconst.CREATED_AT && k != globalconst.UPDATED_AT {
			newDoc = append(newDoc, bson.E{Key: k, Value: v})
		}
	}

	if !now.IsZero() {
		newDoc = append(newDoc, bson.E{Key: globalconst.UPDATED_AT, Value: now})
	}

	return bson.Marshal(newDoc)
}

// HandleCollectionUpdateWhere processes the CmdCollectionUpdateWhere command natively.
func (h *ConnectionHandler) HandleCollectionUpdateWhere(r io.Reader, conn net.Conn) {
	collectionName, queryBSON, patchBSON, err := protocol.ReadCollectionUpdateWhereCommand(r)
	if err != nil || collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED", nil)
		return
	}

	query := queryPool.Get().(*Query)
	defer func() { query.Reset(); queryPool.Put(query) }()
	if err := bson.Unmarshal(queryBSON, query); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid query BSON format", nil)
		return
	}

	// 🔥 USAMOS EL FAST-PATH ZERO-ALLOCATION
	keys, err := h.executeQueryForKeys(collectionName, query)
	if err != nil {
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to execute query", nil)
		return
	}

	if len(keys) == 0 {
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: 0 items matched the condition.", nil)
		return
	}

	var patchData bson.M
	if err := bson.Unmarshal(patchBSON, &patchData); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch format", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	now := time.Now().UTC()
	itemsToUpdate := make(map[string][]byte, len(keys))
	var failedKeys []string

	// Lógica Transaccional
	if h.CurrentTransactionID != "" {
		for _, key := range keys {
			existingValue, found := colStore.Get(key)
			if !found {
				continue
			}

			var existingData bson.M
			bson.Unmarshal(existingValue, &existingData)
			for k, v := range patchData {
				if k != globalconst.ID && k != globalconst.CREATED_AT {
					existingData[k] = v
				}
			}
			finalValue, _ := bson.Marshal(existingData)
			op := store.WriteOperation{Collection: collectionName, Key: key, Value: finalValue, OpType: store.OpTypeUpdate}
			h.TransactionManager.RecordWrite(h.CurrentTransactionID, op)
		}
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d update ops queued in transaction.", len(keys)), nil)
		return
	}

	// Fast Path: Aplicar parches en RAM y encolar
	for _, key := range keys {
		existingValue, found := colStore.Get(key)
		if !found {
			failedKeys = append(failedKeys, key)
			continue
		}

		updatedValue, err := applyBSONPatchFast(existingValue, patchData, now)
		if err != nil {
			failedKeys = append(failedKeys, key)
			continue
		}
		itemsToUpdate[key] = updatedValue
	}

	updatedCount := 0
	if len(itemsToUpdate) > 0 {
		var diskFailedKeys []string
		updatedCount, diskFailedKeys = colStore.UpdateMany(itemsToUpdate)
		failedKeys = append(failedKeys, diskFailedKeys...)
	}

	msg := fmt.Sprintf("OK: %d items updated. %d failed.", updatedCount, len(failedKeys))
	protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
}

// HandleCollectionDeleteWhere processes the CmdCollectionDeleteWhere command natively.
func (h *ConnectionHandler) HandleCollectionDeleteWhere(r io.Reader, conn net.Conn) {
	collectionName, queryBSON, err := protocol.ReadCollectionDeleteWhereCommand(r)
	if err != nil || collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid request", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED", nil)
		return
	}

	query := queryPool.Get().(*Query)
	defer func() { query.Reset(); queryPool.Put(query) }()
	if err := bson.Unmarshal(queryBSON, query); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid query BSON format", nil)
		return
	}

	// 🔥 USAMOS EL FAST-PATH ZERO-ALLOCATION
	keys, err := h.executeQueryForKeys(collectionName, query)
	if err != nil {
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to execute query", nil)
		return
	}

	if len(keys) == 0 {
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: 0 items matched the condition.", nil)
		return
	}

	if h.CurrentTransactionID != "" {
		for _, key := range keys {
			op := store.WriteOperation{Collection: collectionName, Key: key, OpType: store.OpTypeDelete}
			h.TransactionManager.RecordWrite(h.CurrentTransactionID, op)
		}
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d delete ops queued in transaction.", len(keys)), nil)
		return
	}

	// Enviamos el array nativo directamente a la capa de disco
	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.DeleteMany(keys)

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d items deleted.", len(keys)), nil)
}

// enrichBSONForInsertFast inyecta _id y timestamps sin usar reflexión costosa de mapas.
func enrichBSONForInsertFast(rawDoc bson.Raw, id string, now time.Time) []byte {
	elements, _ := rawDoc.Elements()
	newDoc := make(bson.D, 0, len(elements)+3)

	hasID, hasCreated, hasUpdated := false, false, false

	for _, elem := range elements {
		key := elem.Key()
		if key == globalconst.ID {
			hasID = true
		}
		if key == globalconst.CREATED_AT {
			hasCreated = true
		}
		if key == globalconst.UPDATED_AT {
			hasUpdated = true
		}
		newDoc = append(newDoc, bson.E{Key: key, Value: elem.Value()})
	}

	if !hasID {
		newDoc = append(newDoc, bson.E{Key: globalconst.ID, Value: id})
	}
	if !hasCreated {
		newDoc = append(newDoc, bson.E{Key: globalconst.CREATED_AT, Value: now})
	}
	if !hasUpdated {
		newDoc = append(newDoc, bson.E{Key: globalconst.UPDATED_AT, Value: now})
	}

	bytes, _ := bson.Marshal(newDoc)
	return bytes
}
