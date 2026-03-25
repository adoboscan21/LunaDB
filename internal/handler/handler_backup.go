package handler

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"lunadb/internal/protocol"
	"lunadb/internal/store"
)

// handleBackup maneja el comando para un backup manual.
func (h *ConnectionHandler) handleBackup(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	if !h.IsRoot {
		slog.Warn("Unauthorized backup attempt", "user", h.AuthenticatedUser, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a manual backup.", nil)
		}
		return
	}

	slog.Info("Unified logical backup initiated", "user", h.AuthenticatedUser, "remote_addr", remoteAddr)

	baseBackupDir := "backups"
	if err := os.MkdirAll(baseBackupDir, 0755); err != nil {
		slog.Error("Failed to create backups directory", "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to create backups directory.", nil)
		}
		return
	}

	// Ahora generamos un solo archivo .db en lugar de una carpeta
	backupName := fmt.Sprintf("lunadb_backup_%s.db", time.Now().Format("20060102_150405"))
	backupFilePath := filepath.Join(baseBackupDir, backupName)

	if err := store.UnifiedBackup(backupFilePath); err != nil {
		slog.Error("Unified backup failed", "user", h.AuthenticatedUser, "error", err)
		os.Remove(backupFilePath) // Limpiar archivo corrupto
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Backup failed: %v", err), nil)
		}
		return
	}

	slog.Info("Unified backup completed successfully", "user", h.AuthenticatedUser, "file", backupName)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Unified backup completed successfully. Saved as '%s'.", backupName), nil)
	}
}

// HandleRestore maneja el comando para restaurar desde un backup.
func (h *ConnectionHandler) HandleRestore(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	if conn != nil {
		if !h.IsRoot {
			slog.Warn("Unauthorized restore attempt", "user", h.AuthenticatedUser, "remote_addr", remoteAddr)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a restore.", nil)
			return
		}
	}

	rawBackupName, err := protocol.ReadRestoreCommand(r)
	if err != nil {
		slog.Error("Failed to read RESTORE command payload", "remote_addr", remoteAddr, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid RESTORE command format.", nil)
		}
		return
	}

	backupName := filepath.Base(filepath.Clean(rawBackupName))

	if backupName == "" || backupName == "." || backupName == "/" || backupName == "\\" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid or empty backup name.", nil)
		}
		return
	}

	backupFilePath := filepath.Join("backups", backupName)
	if _, err := os.Stat(backupFilePath); os.IsNotExist(err) {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("ERROR: Backup file '%s' not found.", backupName), nil)
		}
		return
	}

	slog.Warn("DESTRUCTIVE ACTION: Unified Restore & Re-Sharding initiated", "user", h.AuthenticatedUser, "backup_name", backupName, "remote_addr", remoteAddr)

	// 1. Purgar el caché de la RAM (¡SIN borrar los discos!)
	h.CollectionManager.ClearMemory()

	// 2. Invocamos la lógica que vacía y re-distribuye los discos
	if err := store.UnifiedRestore(backupFilePath); err != nil {
		slog.Error("Restore failed during data transfer", "backup_name", backupName, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Restore failed: %v", err), nil)
		}
		return
	}

	// 3. Reconstruir la RAM leyendo la nueva estructura de los discos
	if err := h.CollectionManager.InitializeFromDisk(); err != nil {
		slog.Error("Failed to rebuild indexes after restore", "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Data restored but index rebuild failed. Restart recommended.", nil)
		}
		return
	}

	slog.Info("Restore completed successfully", "backup_name", backupName, "user", h.AuthenticatedUser)
	if conn != nil {
		msg := fmt.Sprintf("OK: Restore from file '%s' completed successfully. All shards re-balanced and ready.", backupName)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
	}
}
