/* ==========================================================
   Ruta y Archivo: ./internal/handler/collection_item_commands.go
   ========================================================== */

package handler

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"lunadb/internal/globalconst"
	"lunadb/internal/persistence"
	"lunadb/internal/protocol"
	"lunadb/internal/store"
	"lunadb/internal/wal"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

// HandleCollectionItemSet processes the CmdCollectionItemSet command. It is a write operation.
func (h *ConnectionHandler) HandleCollectionItemSet(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, key, value, ttl, err := protocol.ReadCollectionItemSetCommand(r)
	if err != nil {
		slog.Error("Failed to read COLLECTION_ITEM_SET command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
		}
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	wasKeyGenerated := false

	if key == "" {
		if conn == nil {
			slog.Error("CRITICAL: SET command with empty key received during WAL replay.", "collection", collectionName)
			return
		}

		wasKeyGenerated = true
		const maxGenerateRetries = 5
		for i := 0; i < maxGenerateRetries; i++ {
			newKey := uuid.New().String()

			_, foundInMem := colStore.Get(newKey)
			if foundInMem {
				continue
			}

			foundInCold, err := persistence.CheckColdKeyExists(collectionName, newKey)
			if err != nil {
				slog.Error("Failed to check key existence in cold storage", "collection", collectionName, "key", newKey, "error", err)
				protocol.WriteResponse(conn, protocol.StatusError, "Internal server error during key uniqueness check.", nil)
				return
			}
			if foundInCold {
				continue
			}

			key = newKey
			break
		}

		if key == "" {
			errMessage := "Failed to generate a unique ID after several attempts. This indicates a highly unusual system state or high key collision rate."
			slog.Error(errMessage, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusError, errMessage, nil)
			return
		}
	}

	if conn != nil && !wasKeyGenerated && h.CurrentTransactionID == "" {
		_, foundInMem := colStore.Get(key)
		foundInCold, err := persistence.CheckColdKeyExists(collectionName, key)
		if err != nil {
			slog.Error("Failed to check key existence in cold storage for client-provided key", "collection", collectionName, "key", key, "error", err)
			protocol.WriteResponse(conn, protocol.StatusError, "Internal server error during key validation.", nil)
			return
		}

		if foundInMem || foundInCold {
			slog.Warn("Set item failed because key already exists", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Key '%s' already exists in collection '%s'. Use 'collection item update' to modify.", key, collectionName), nil)
			return
		}
	}

	if conn != nil {
		if collectionName == "" || len(value) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item set attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if h.CurrentTransactionID == "" && !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Set item failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist. Please create it first.", collectionName), nil)
			return
		}
	}

	var data bson.M
	if err := bson.Unmarshal(value, &data); err != nil {
		slog.Warn("Failed to unmarshal BSON item data for SET", "error", err, "collection", collectionName, "user", h.AuthenticatedUser)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid value. Must be a BSON object.", nil)
		}
		return
	}
	data[globalconst.ID] = key

	// Transactional logic
	if h.CurrentTransactionID != "" {
		finalValueForTx, err := bson.Marshal(data)
		if err != nil {
			slog.Error("Failed to marshal enriched value for transaction", "key", key, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "Internal server error preparing data for transaction", nil)
			}
			return
		}

		op := store.WriteOperation{
			Collection: collectionName,
			Key:        key,
			Value:      finalValueForTx,
			OpType:     store.OpTypeSet,
		}

		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record operation in transaction: "+err.Error(), nil)
			}
			return
		}

		if conn != nil && h.Wal != nil {
			var buf bytes.Buffer
			protocol.WriteCollectionItemSetCommand(&buf, collectionName, key, finalValueForTx, ttl)
			h.Wal.Write(wal.WalEntry{
				CommandType: protocol.CmdCollectionItemSet,
				Payload:     buf.Bytes()[1:],
			})
		}

		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, "OK: Operation queued in transaction.", finalValueForTx)
		}
		return
	}

	// Non-transactional logic
	if conn != nil {
		now := time.Now().UTC()
		data[globalconst.UPDATED_AT] = now
		data[globalconst.CREATED_AT] = now
	}

	finalValue, err := bson.Marshal(data)
	if err != nil {
		slog.Error("Failed to marshal final value with timestamps", "key", key, "collection", collectionName, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to process value with timestamps", nil)
		}
		return
	}

	if conn != nil && h.Wal != nil {
		var buf bytes.Buffer
		protocol.WriteCollectionItemSetCommand(&buf, collectionName, key, finalValue, ttl)
		h.Wal.Write(wal.WalEntry{
			CommandType: protocol.CmdCollectionItemSet,
			Payload:     buf.Bytes()[1:],
		})
	}

	colStore.Set(key, finalValue, ttl)
	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Item set in collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key, "operation", "create")
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s' (persistence async)", key, collectionName), finalValue)
	}
}

// HandleCollectionItemUpdate processes the CmdCollectionItemUpdate command. It is a write operation.
func (h *ConnectionHandler) HandleCollectionItemUpdate(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, key, patchValue, err := protocol.ReadCollectionItemUpdateCommand(r)
	if err != nil {
		slog.Error("Failed to read COLLECTION_ITEM_UPDATE command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_UPDATE command format", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || key == "" || len(patchValue) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name, key, or patch value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item update attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Update item failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Transactional logic
	if h.CurrentTransactionID != "" {
		colStore := h.CollectionManager.GetCollection(collectionName)
		existingValue, found := colStore.Get(key)
		if !found {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusNotFound, "Item not found in memory. Updates inside a transaction currently only support hot data.", nil)
			}
			return
		}
		var existingData, patchData bson.M
		if err := bson.Unmarshal(existingValue, &existingData); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "Failed to unmarshal existing document for update.", nil)
			}
			return
		}
		if err := bson.Unmarshal(patchValue, &patchData); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch BSON format.", nil)
			}
			return
		}
		for k, v := range patchData {
			if k != globalconst.ID && k != globalconst.CREATED_AT {
				existingData[k] = v
			}
		}
		finalValue, _ := bson.Marshal(existingData)

		op := store.WriteOperation{
			Collection: collectionName,
			Key:        key,
			Value:      finalValue,
			OpType:     store.OpTypeUpdate,
		}

		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record update in transaction: "+err.Error(), nil)
			}
			return
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, "OK: Update operation queued in transaction.", nil)
		}
		return
	}

	// Non-transactional logic (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	if existingValue, found := colStore.Get(key); found {
		var patchData bson.M
		if err := bson.Unmarshal(patchValue, &patchData); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch BSON format.", nil)
			}
			return
		}

		// USAMOS EL FAST PATH
		var updatedValue []byte
		var applyErr error
		if conn != nil {
			updatedValue, applyErr = applyBSONPatchFast(existingValue, patchData, time.Now().UTC())
		} else {
			updatedValue, applyErr = applyBSONPatchFast(existingValue, patchData, time.Time{})
		}

		if applyErr != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "Failed to apply patch.", nil)
			}
			return
		}

		// NUEVO: Usar el método Update optimizado en lugar de Set
		colStore.Update(key, updatedValue)
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
		slog.Info("Item updated in collection (hot)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' updated in collection '%s'", key, collectionName), updatedValue)
		}
		return
	}

	fileLock := h.CollectionManager.GetFileLock(collectionName)
	fileLock.Lock()
	updated, err := persistence.UpdateColdItem(collectionName, key, patchValue)
	fileLock.Unlock()

	if err != nil {
		slog.Error("Failed to update cold item on disk", "collection", collectionName, "key", key, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to update item on disk", nil)
		}
		return
	}
	if !updated {
		slog.Warn("Item update failed: key not found", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection '%s'", key, collectionName), nil)
		}
		return
	}
	slog.Info("Item updated in collection (cold)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Cold item '%s' updated in collection '%s'", key, collectionName), nil)
	}
}

type updateManyPayload struct {
	ID    string `bson:"_id"`
	Patch bson.M `bson:"patch"`
}

// HandleCollectionItemUpdateMany processes the CmdCollectionItemUpdateMany command. It is a write operation.
func (h *ConnectionHandler) HandleCollectionItemUpdateMany(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, value, err := protocol.ReadCollectionItemUpdateManyCommand(r)
	if err != nil {
		slog.Error("Failed to read UPDATE_MANY command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid UPDATE_COLLECTION_ITEMS_MANY command format", nil)
		}
		return
	}

	// Extraer el arreglo del envoltorio BSON {"array": [...]}
	var payload struct {
		Array []updateManyPayload `bson:"array"`
	}
	if err := bson.Unmarshal(value, &payload); err != nil {
		slog.Warn("Failed to unmarshal BSON array for UPDATE_MANY", "collection", collectionName, "error", err, "user", h.AuthenticatedUser)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid BSON array format.", nil)
		}
		return
	}
	payloads := payload.Array

	if conn != nil {
		if collectionName == "" || len(value) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item update-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Update-many failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Transactional logic
	if h.CurrentTransactionID != "" {
		colStore := h.CollectionManager.GetCollection(collectionName)
		for _, p := range payloads {
			existingValue, found := colStore.Get(p.ID)
			if !found {
				if conn != nil {
					protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("Item '%s' not found in memory for transactional update.", p.ID), nil)
				}
				return
			}
			var existingData bson.M
			bson.Unmarshal(existingValue, &existingData)
			for k, v := range p.Patch {
				if k != globalconst.ID && k != globalconst.CREATED_AT {
					existingData[k] = v
				}
			}
			finalValue, _ := bson.Marshal(existingData)

			op := store.WriteOperation{
				Collection: collectionName,
				Key:        p.ID,
				Value:      finalValue,
				OpType:     store.OpTypeUpdate,
			}

			if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
				if conn != nil {
					protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record update-many op: "+err.Error(), nil)
				}
				return
			}
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d update operations queued in transaction.", len(payloads)), nil)
		}
		return
	}

	// Non-transactional logic (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	var hotPayloads []updateManyPayload
	var coldPayloads []persistence.ColdUpdatePayload
	for _, p := range payloads {
		if _, found := colStore.Get(p.ID); found {
			hotPayloads = append(hotPayloads, p)
		} else {
			coldPayloads = append(coldPayloads, persistence.ColdUpdatePayload{ID: p.ID, Patch: p.Patch})
		}
	}
	slog.Debug("Split update-many batch", "hot_count", len(hotPayloads), "cold_count", len(coldPayloads))
	updatedHotCount := 0
	var failedHotKeys []string

	now := time.Now().UTC()
	for _, p := range hotPayloads {
		existingValue, _ := colStore.Get(p.ID)

		// USAMOS EL FAST PATH
		var updatedValue []byte
		var err error
		if conn != nil {
			updatedValue, err = applyBSONPatchFast(existingValue, p.Patch, now)
		} else {
			updatedValue, err = applyBSONPatchFast(existingValue, p.Patch, time.Time{})
		}

		if err != nil {
			failedHotKeys = append(failedHotKeys, p.ID)
			continue
		}

		colStore.Update(p.ID, updatedValue)
		updatedHotCount++
	}
	if updatedHotCount > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}
	updatedColdCount := 0
	if len(coldPayloads) > 0 {
		fileLock := h.CollectionManager.GetFileLock(collectionName)
		fileLock.Lock()
		count, err := persistence.UpdateManyColdItems(collectionName, coldPayloads)
		fileLock.Unlock()
		if err != nil {
			slog.Error("Failed to update cold items batch", "collection", collectionName, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "An error occurred during the cold batch update.", nil)
			}
			return
		}
		updatedColdCount = count
	}
	totalUpdated := updatedHotCount + updatedColdCount
	totalFailed := (len(payloads) - totalUpdated)
	slog.Info("Update-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "updated_count", totalUpdated, "failed_count", totalFailed)
	if conn != nil {
		summary := fmt.Sprintf("OK: %d items updated. %d items failed or not found.", totalUpdated, totalFailed)
		var responseData []byte
		if len(failedHotKeys) > 0 {
			responseData, _ = bson.Marshal(bson.M{"failed_hot_keys": failedHotKeys})
		}
		protocol.WriteResponse(conn, protocol.StatusOk, summary, responseData)
	}
}

// handleCollectionItemGet processes the CmdCollectionItemGet command. It is a read-only operation.
func (h *ConnectionHandler) handleCollectionItemGet(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Read operations like GET are not supported inside a transaction in this version.", nil)
		return
	}
	collectionName, key, err := protocol.ReadCollectionItemGetCommand(r)
	if err != nil {
		slog.Error("Failed to read GET_ITEM command payload", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_GET command format", nil)
		return
	}
	if collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
		slog.Warn("Unauthorized collection item get attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}
	colStore := h.CollectionManager.GetCollection(collectionName)
	value, found := colStore.Get(key)
	slog.Debug("Get item from collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key, "found", found)
	if found {
		if collectionName == globalconst.SystemCollectionName && strings.HasPrefix(key, globalconst.UserPrefix) {
			var userInfo UserInfo
			if err := bson.Unmarshal(value, &userInfo); err == nil {
				sanitizedInfo := bson.M{
					"username":    userInfo.Username,
					"is_root":     userInfo.IsRoot,
					"permissions": userInfo.Permissions,
				}
				sanitizedBytes, _ := bson.Marshal(sanitizedInfo)
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s' (sanitized)", key, collectionName), sanitizedBytes)
				return
			}
		}
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s'", key, collectionName), value)
	} else {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in collection '%s'", key, collectionName), nil)
	}
}

// HandleCollectionItemDelete processes the CmdCollectionItemDelete command. It is a write operation.
func (h *ConnectionHandler) HandleCollectionItemDelete(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, key, err := protocol.ReadCollectionItemDeleteCommand(r)
	if err != nil {
		slog.Error("Failed to read DELETE_ITEM command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_DELETE command format", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || key == "" {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item delete attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Delete item failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Transactional logic
	if h.CurrentTransactionID != "" {
		op := store.WriteOperation{
			Collection: collectionName,
			Key:        key,
			OpType:     store.OpTypeDelete,
		}

		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record delete in transaction: "+err.Error(), nil)
			}
			return
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, "OK: Delete operation queued in transaction.", nil)
		}
		return
	}

	// Non-transactional logic (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	if _, foundInRam := colStore.Get(key); foundInRam {
		colStore.Delete(key)
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
		slog.Info("Item deleted from collection (hot)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s'", key, collectionName), nil)
		}
		return
	}

	fileLock := h.CollectionManager.GetFileLock(collectionName)
	fileLock.Lock()
	marked, err := persistence.DeleteColdItem(collectionName, key)
	fileLock.Unlock()

	if err != nil {
		slog.Error("Failed to mark item as deleted on disk", "collection", collectionName, "key", key, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to perform delete operation on disk", nil)
		}
		return
	}
	if !marked {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection", key), nil)
		}
		return
	}
	slog.Info("Item marked for deletion in collection (cold)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' marked for deletion from collection '%s'", key, collectionName), nil)
	}
}

// handleCollectionItemList processes the CmdCollectionItemList command. It is a read-only operation.
func (h *ConnectionHandler) handleCollectionItemList(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: LIST command is not allowed inside a transaction in this version.", nil)
		return
	}
	collectionName, err := protocol.ReadCollectionItemListCommand(r)
	if err != nil {
		slog.Error("Failed to read LIST_ITEMS command payload", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_LIST command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}
	if !h.IsRoot || !h.IsLocalhostConn {
		slog.Warn("Unauthorized list-all-items attempt", "user", h.AuthenticatedUser, "collection", collectionName, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Listing all items is a privileged operation for root@localhost.", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
		slog.Warn("Unauthorized collection item list attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}
	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for listing items", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	allData := colStore.GetAll()

	// MEJORA BSON: Preparamos un array para estructurar la respuesta en formato de tabla
	resultsArray := make([]bson.M, 0, len(allData))

	if collectionName == globalconst.SystemCollectionName {
		for key, val := range allData {
			if strings.HasPrefix(key, globalconst.UserPrefix) {
				var userInfo UserInfo
				if err := bson.Unmarshal(val, &userInfo); err == nil {
					doc := bson.M{
						globalconst.ID: key,
						"username":     userInfo.Username,
						"is_root":      userInfo.IsRoot,
						"permissions":  userInfo.Permissions,
					}
					resultsArray = append(resultsArray, doc)
				}
			} else {
				resultsArray = append(resultsArray, bson.M{globalconst.ID: key, "data": "non-user system data (omitted)"})
			}
		}
	} else {
		for key, val := range allData {
			var doc bson.M
			if err := bson.Unmarshal(val, &doc); err == nil {
				// Aseguramos que el ID siempre esté presente para imprimirlo
				if _, ok := doc[globalconst.ID]; !ok {
					doc[globalconst.ID] = key
				}
				resultsArray = append(resultsArray, doc)
			}
		}
	}

	// Envolvemos el array en un objeto raíz válido para BSON
	bsonResponseData, err := bson.Marshal(bson.M{"results": resultsArray})
	if err != nil {
		slog.Error("Failed to marshal collection items to BSON", "collection", collectionName, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection items", nil)
		return
	}

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Items from collection '%s' retrieved", collectionName), bsonResponseData)
	slog.Info("All items listed from collection", "user", h.AuthenticatedUser, "collection", collectionName, "item_count", len(allData))
}

// HandleCollectionItemSetMany processes the CmdCollectionItemSetMany command using Lazy Decoding for maximum throughput.
func (h *ConnectionHandler) HandleCollectionItemSetMany(r io.Reader, conn net.Conn) {
	// Truco ninja para evadir el falso positivo del linter "nilness"
	isClient := any(conn) != nil
	remoteAddr := "recovery"

	if isClient {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, value, err := protocol.ReadCollectionItemSetManyCommand(r)
	if err != nil {
		slog.Error("Failed to read SET_MANY command payload", "error", err, "remote_addr", remoteAddr)
		if isClient {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET_COLLECTION_ITEMS_MANY command format", nil)
		}
		return
	}

	if isClient {
		if collectionName == "" || len(value) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item set-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if h.CurrentTransactionID == "" && !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Set-many operation failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist. Please create it first.", collectionName), nil)
			return
		}
	}

	// --- OPTIMIZACIÓN: Lazy Decoding (Zero-Copy) ---
	rawWrapper := bson.Raw(value)
	rawArray, err := rawWrapper.LookupErr("array")
	if err != nil {
		slog.Warn("Failed to find 'array' wrapper for SET_MANY", "collection", collectionName, "error", err)
		if isClient {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid BSON format: missing 'array' key", nil)
		}
		return
	}

	elements, err := rawArray.Array().Elements()
	if err != nil {
		slog.Warn("Invalid BSON array format", "error", err)
		if isClient {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid BSON array format", nil)
		}
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	recordsToProcess := make([]bson.Raw, 0, len(elements))
	duplicateKeys := make([]string, 0)
	invalidRecordsCount := 0
	clientProvidedKeys := make([]string, 0, len(elements))

	// Primer pase: Extracción de llaves (Rápido, sin unmarshal profundo)
	for _, elem := range elements {
		doc := elem.Value().Document()
		idVal, err := doc.LookupErr(globalconst.ID)
		if err == nil && idVal.Type == bson.TypeString {
			key := idVal.StringValue()
			if key != "" {
				clientProvidedKeys = append(clientProvidedKeys, key)
			}
		}
	}

	var foundInCold map[string]bool
	if h.CurrentTransactionID == "" && len(clientProvidedKeys) > 0 {
		var checkErr error
		foundInCold, checkErr = persistence.CheckManyColdKeysExist(collectionName, clientProvidedKeys)
		if checkErr != nil {
			slog.Error("Failed to check batch key existence in cold storage", "collection", collectionName, "error", checkErr)
			if isClient {
				protocol.WriteResponse(conn, protocol.StatusError, "Internal server error during batch key validation.", nil)
			}
			return
		}
	}

	now := time.Now().UTC()

	// Segundo pase: Procesamiento y creación de los nuevos documentos BSON crudos
	for _, elem := range elements {
		doc := elem.Value().Document()
		var key string

		idVal, err := doc.LookupErr(globalconst.ID)
		if err == nil && idVal.Type == bson.TypeString {
			key = idVal.StringValue()
		}

		if key == "" {
			if !isClient {
				invalidRecordsCount++
				continue
			}

			// Generar ID
			generatedKey := ""
			const maxGenerateRetries = 5
			for i := 0; i < maxGenerateRetries; i++ {
				newKey := uuid.New().String()
				_, foundInMem := colStore.Get(newKey)
				foundInColdGen, _ := persistence.CheckColdKeyExists(collectionName, newKey)
				if !foundInMem && !foundInColdGen {
					generatedKey = newKey
					break
				}
			}

			if generatedKey == "" {
				invalidRecordsCount++
				continue
			}
			key = generatedKey

			var m bson.M
			bson.Unmarshal(doc, &m)
			m[globalconst.ID] = key
			if isClient {
				m[globalconst.CREATED_AT] = now
				m[globalconst.UPDATED_AT] = now
			}
			newDocBytes, _ := bson.Marshal(m)
			recordsToProcess = append(recordsToProcess, bson.Raw(newDocBytes))
			continue
		}

		if h.CurrentTransactionID == "" {
			_, existsInMem := colStore.Get(key)
			_, existsInCold := foundInCold[key]

			if existsInMem || existsInCold {
				duplicateKeys = append(duplicateKeys, key)
			} else {
				var m bson.M
				bson.Unmarshal(doc, &m)
				if isClient {
					m[globalconst.CREATED_AT] = now
					m[globalconst.UPDATED_AT] = now
				}
				newDocBytes, _ := bson.Marshal(m)
				recordsToProcess = append(recordsToProcess, bson.Raw(newDocBytes))
			}
		} else {
			var m bson.M
			bson.Unmarshal(doc, &m)
			if isClient {
				m[globalconst.CREATED_AT] = now
				m[globalconst.UPDATED_AT] = now
			}
			newDocBytes, _ := bson.Marshal(m)
			recordsToProcess = append(recordsToProcess, bson.Raw(newDocBytes))
		}
	}

	if len(recordsToProcess) == 0 && isClient && h.CurrentTransactionID == "" {
		msg := fmt.Sprintf("OK: 0 items processed. %d records were skipped due to existing keys and %d were invalid.", len(duplicateKeys), invalidRecordsCount)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
		return
	}

	// Transactional logic
	if h.CurrentTransactionID != "" {
		for _, rawDoc := range recordsToProcess {
			idVal, _ := rawDoc.LookupErr(globalconst.ID)
			key := idVal.StringValue()

			op := store.WriteOperation{
				Collection: collectionName, Key: key, Value: rawDoc, OpType: store.OpTypeSet,
			}
			if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
				if isClient {
					protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record set-many op in transaction: "+err.Error(), nil)
				}
				return
			}
		}

		if isClient && h.Wal != nil && len(recordsToProcess) > 0 {
			finalDocsBytes, _ := bson.Marshal(bson.M{"array": recordsToProcess})
			var buf bytes.Buffer
			protocol.WriteCollectionItemSetManyCommand(&buf, collectionName, finalDocsBytes)
			h.Wal.Write(wal.WalEntry{
				CommandType: protocol.CmdCollectionItemSetMany,
				Payload:     buf.Bytes()[1:],
			})
		}

		if isClient {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d set operations queued in transaction.", len(recordsToProcess)), nil)
		}
		return
	}

	// Non-transactional logic
	if isClient && h.Wal != nil && len(recordsToProcess) > 0 {
		finalDocsBytes, _ := bson.Marshal(bson.M{"array": recordsToProcess})
		var buf bytes.Buffer
		protocol.WriteCollectionItemSetManyCommand(&buf, collectionName, finalDocsBytes)
		h.Wal.Write(wal.WalEntry{
			CommandType: protocol.CmdCollectionItemSetMany,
			Payload:     buf.Bytes()[1:],
		})
	}

	for _, rawDoc := range recordsToProcess {
		idVal, _ := rawDoc.LookupErr(globalconst.ID)
		key := idVal.StringValue()
		colStore.Set(key, rawDoc, 0)
	}

	if len(recordsToProcess) > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}

	slog.Info("Set-many operation completed", "user", h.AuthenticatedUser, "inserted_count", len(recordsToProcess), "duplicates_skipped", len(duplicateKeys), "invalid_skipped", invalidRecordsCount)

	if isClient {
		msg := fmt.Sprintf("OK: %d items set in collection '%s'. %d records were skipped due to existing keys. %d were invalid.", len(recordsToProcess), collectionName, len(duplicateKeys), invalidRecordsCount)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
	}
}

// HandleCollectionItemDeleteMany processes the CmdCollectionItemDeleteMany command. It is a write operation.
func (h *ConnectionHandler) HandleCollectionItemDeleteMany(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, keys, err := protocol.ReadCollectionItemDeleteManyCommand(r)
	if err != nil {
		slog.Error("Failed to read DELETE_MANY command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_ITEMS_MANY command format", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || len(keys) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty and keys must be provided", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item delete-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Delete-many failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Transactional logic
	if h.CurrentTransactionID != "" {
		for _, key := range keys {
			op := store.WriteOperation{
				Collection: collectionName,
				Key:        key,
				OpType:     store.OpTypeDelete,
			}

			if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
				if conn != nil {
					protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record delete-many op in transaction: "+err.Error(), nil)
				}
				return
			}
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d delete operations queued in transaction.", len(keys)), nil)
		}
		return
	}

	// Non-transactional logic (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	var hotKeysToDelete, coldKeysToTombstone []string
	for _, key := range keys {
		if _, foundInRam := colStore.Get(key); foundInRam {
			hotKeysToDelete = append(hotKeysToDelete, key)
		} else {
			coldKeysToTombstone = append(coldKeysToTombstone, key)
		}
	}
	if len(hotKeysToDelete) > 0 {
		for _, key := range hotKeysToDelete {
			colStore.Delete(key)
		}
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}
	var markedCount int
	if len(coldKeysToTombstone) > 0 {
		fileLock := h.CollectionManager.GetFileLock(collectionName)
		fileLock.Lock()
		markedCount, err = persistence.DeleteManyColdItems(collectionName, coldKeysToTombstone)
		fileLock.Unlock()
		if err != nil {
			slog.Error("Failed to mark items for deletion in cold storage", "collection", collectionName, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "An error occurred during the batch delete operation.", nil)
			}
			return
		}
	}
	totalProcessed := len(hotKeysToDelete) + markedCount
	slog.Info("Delete-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "processed_count", totalProcessed)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys processed for deletion from collection '%s'.", totalProcessed, collectionName), nil)
	}
}

// applyBSONPatchFast aplica un parche BSON directamente sobre los bytes crudos (Zero-Copy)
// preservando los tipos originales sin usar reflection pesada en los campos no modificados.
func applyBSONPatchFast(existingValue []byte, patchData bson.M, now time.Time) ([]byte, error) {
	rawDoc := bson.Raw(existingValue)
	elements, err := rawDoc.Elements()
	if err != nil {
		return nil, err
	}

	newDoc := make(bson.D, 0, len(elements)+len(patchData)+1)
	patchedKeys := make(map[string]bool)

	// 1. Recorrer documento original y copiar bytes directamente
	for _, elem := range elements {
		key := elem.Key()
		// Proteger campos inmutables
		if key == globalconst.ID || key == globalconst.CREATED_AT {
			newDoc = append(newDoc, bson.E{Key: key, Value: elem.Value()})
			continue
		}
		if key == globalconst.UPDATED_AT {
			continue // Se actualizará al final
		}

		// Si el campo está en el parche, usar el nuevo valor
		if patchVal, exists := patchData[key]; exists {
			newDoc = append(newDoc, bson.E{Key: key, Value: patchVal})
			patchedKeys[key] = true
		} else {
			// Mantener valor original tal cual en bytes crudos (Ultra rápido)
			newDoc = append(newDoc, bson.E{Key: key, Value: elem.Value()})
		}
	}

	// 2. Agregar campos nuevos del parche que no existían
	for k, v := range patchData {
		if !patchedKeys[k] && k != globalconst.ID && k != globalconst.CREATED_AT && k != globalconst.UPDATED_AT {
			newDoc = append(newDoc, bson.E{Key: k, Value: v})
		}
	}

	// 3. Actualizar timestamp
	if !now.IsZero() {
		newDoc = append(newDoc, bson.E{Key: globalconst.UPDATED_AT, Value: now})
	}

	return bson.Marshal(newDoc)
}
