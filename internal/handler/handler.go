/* ==========================================================
   Ruta y Archivo: ./internal/handler/handler.go
   ========================================================== */

package handler

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"lunadb/internal/protocol"
	"lunadb/internal/store"
)

type ActivityUpdater interface {
	UpdateActivity()
}

// ConnectionHandler manages a single client connection, including state like authentication and transactions.
type ConnectionHandler struct {
	MainStore            store.DataStore
	CollectionManager    *store.CollectionManager
	ActivityUpdater      ActivityUpdater
	IsAuthenticated      bool
	AuthenticatedUser    string
	IsLocalhostConn      bool
	IsRoot               bool
	Permissions          map[string]string
	TransactionManager   *store.TransactionManager
	CurrentTransactionID string
}

var connectionHandlerPool = sync.Pool{
	New: func() any {
		return &ConnectionHandler{
			Permissions: make(map[string]string),
		}
	},
}

// Reset clears the handler's state for reuse from the pool.
func (h *ConnectionHandler) Reset() {
	h.MainStore = nil
	h.CollectionManager = nil
	h.ActivityUpdater = nil
	h.IsAuthenticated = false
	h.AuthenticatedUser = ""
	h.IsLocalhostConn = false
	h.IsRoot = false
	clear(h.Permissions)
	h.TransactionManager = nil
	h.CurrentTransactionID = ""
}

// GetConnectionHandlerFromPool retrieves a handler from the pool and initializes it.
func GetConnectionHandlerFromPool(
	mainStore store.DataStore,
	colManager *store.CollectionManager,
	txManager *store.TransactionManager,
	updater ActivityUpdater,
	conn net.Conn,
) *ConnectionHandler {
	h := connectionHandlerPool.Get().(*ConnectionHandler)

	isLocal := false
	if conn != nil {
		if host, _, err := net.SplitHostPort(conn.RemoteAddr().String()); err == nil {
			if host == "127.0.0.1" || host == "::1" || host == "localhost" {
				isLocal = true
			}
		}
	}

	h.MainStore = mainStore
	h.CollectionManager = colManager
	h.TransactionManager = txManager
	h.ActivityUpdater = updater
	h.IsLocalhostConn = isLocal

	return h
}

// PutConnectionHandlerToPool returns a handler to the pool after use.
func PutConnectionHandlerToPool(h *ConnectionHandler) {
	if h.CurrentTransactionID != "" {
		slog.Warn("Connection closed mid-transaction, rolling back.", "txID", h.CurrentTransactionID)
		h.TransactionManager.Rollback(h.CurrentTransactionID)
	}
	h.Reset()
	connectionHandlerPool.Put(h)
}

// HandleConnection is the main loop for processing commands from a single connection.
func (h *ConnectionHandler) HandleConnection(conn net.Conn) {
	defer conn.Close()
	slog.Info("New client connected", "remote_addr", conn.RemoteAddr().String(), "is_localhost", h.IsLocalhostConn)

	for {
		cmdType, err := protocol.ReadCommandType(conn)
		if err != nil {
			if err != io.EOF {
				slog.Error("Failed to read command type", "remote_addr", conn.RemoteAddr().String(), "error", err)
			} else {
				slog.Info("Client disconnected", "remote_addr", conn.RemoteAddr().String())
			}
			return
		}

		h.ActivityUpdater.UpdateActivity()

		// Extraemos el payload completo del comando actual de la red de forma segura.
		// Esto evita que si hay un error de lógica, dejemos bytes basura en el stream TCP.
		payload, err := protocol.ReadCommandPayload(conn, cmdType)
		if err != nil {
			slog.Error("Failed to read command payload", "error", err, "command_type", cmdType)
			protocol.WriteResponse(conn, protocol.StatusError, "Internal server error reading command", nil)
			continue
		}

		// Envolvemos el payload en un reader en memoria para que los handlers específicos lo consuman
		var reader io.Reader = bytes.NewReader(payload)

		if cmdType == protocol.CmdAuthenticate {
			h.handleAuthenticate(reader, conn)
			continue
		}

		if !h.IsAuthenticated {
			slog.Warn("Unauthorized access attempt", "remote_addr", conn.RemoteAddr().String(), "command_type", cmdType)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Please authenticate first.", nil)
			// Al estar el payload envuelto en un bytes.NewReader, se descarta automáticamente al seguir el loop.
			continue
		}

		switch cmdType {
		case protocol.CmdBegin:
			h.handleBegin(reader, conn)
		case protocol.CmdCommit:
			h.HandleCommit(reader, conn)
		case protocol.CmdRollback:
			h.handleRollback(reader, conn)
		case protocol.CmdSet:
			h.HandleMainStoreSet(reader, conn)
		case protocol.CmdGet:
			h.handleMainStoreGet(reader, conn)
		case protocol.CmdCollectionCreate:
			h.HandleCollectionCreate(reader, conn)
		case protocol.CmdCollectionDelete:
			h.HandleCollectionDelete(reader, conn)
		case protocol.CmdCollectionList:
			h.handleCollectionList(reader, conn)
		case protocol.CmdCollectionIndexCreate:
			h.HandleCollectionIndexCreate(reader, conn)
		case protocol.CmdCollectionIndexDelete:
			h.HandleCollectionIndexDelete(reader, conn)
		case protocol.CmdCollectionIndexList:
			h.handleCollectionIndexList(reader, conn)
		case protocol.CmdCollectionItemSet:
			h.HandleCollectionItemSet(reader, conn)
		case protocol.CmdCollectionItemSetMany:
			h.HandleCollectionItemSetMany(reader, conn)
		case protocol.CmdCollectionItemDeleteMany:
			h.HandleCollectionItemDeleteMany(reader, conn)
		case protocol.CmdCollectionItemGet:
			h.handleCollectionItemGet(reader, conn)
		case protocol.CmdCollectionItemDelete:
			h.HandleCollectionItemDelete(reader, conn)
		case protocol.CmdCollectionItemList:
			h.handleCollectionItemList(reader, conn)
		case protocol.CmdCollectionItemUpdate:
			h.HandleCollectionItemUpdate(reader, conn)
		case protocol.CmdCollectionItemUpdateMany:
			h.HandleCollectionItemUpdateMany(reader, conn)
		case protocol.CmdCollectionQuery:
			h.handleCollectionQuery(reader, conn)
		case protocol.CmdChangeUserPassword:
			h.HandleChangeUserPassword(reader, conn)
		case protocol.CmdUserCreate:
			h.HandleUserCreate(reader, conn)
		case protocol.CmdUserUpdate:
			h.HandleUserUpdate(reader, conn)
		case protocol.CmdUserDelete:
			h.HandleUserDelete(reader, conn)
		case protocol.CmdBackup:
			h.handleBackup(reader, conn)
		case protocol.CmdRestore:
			h.HandleRestore(reader, conn)
		default:
			slog.Warn("Received unhandled command type", "command_type", cmdType, "remote_addr", conn.RemoteAddr().String())
			protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unhandled or unknown command type %d", cmdType), nil)
		}
	}
}
