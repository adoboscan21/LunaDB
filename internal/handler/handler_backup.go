/* ==========================================================
   Ruta y Archivo: ./internal/handler/handler_backup.go
   ========================================================== */

package handler

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"

	"lunadb/internal/protocol"
	"lunadb/internal/store"
)

// handleBackup maneja el comando para un backup manual.
// Gracias a bbolt, esto se hace en caliente (Hot Backup) sin bloquear la base de datos.
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

	slog.Info("Manual backup initiated", "user", h.AuthenticatedUser, "remote_addr", remoteAddr)

	// 1. Crear directorio de backups si no existe
	backupDir := "backups"
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		slog.Error("Failed to create backups directory", "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to create backups directory.", nil)
		return
	}

	// 2. Generar nombre de archivo único
	fileName := fmt.Sprintf("lunadb_backup_%s.db", time.Now().Format("20060102_150405"))
	backupPath := filepath.Join(backupDir, fileName)

	file, err := os.Create(backupPath)
	if err != nil {
		slog.Error("Failed to create backup file", "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to create backup file.", nil)
		return
	}
	defer file.Close()

	// 3. Volcado nativo de bbolt (Hot Backup)
	err = store.GlobalDB.View(func(tx *bbolt.Tx) error {
		_, writeErr := tx.WriteTo(file)
		return writeErr
	})

	if err != nil {
		slog.Error("Manual backup failed during WriteTo", "user", h.AuthenticatedUser, "error", err)
		os.Remove(backupPath) // Limpiar archivo corrupto
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Backup failed: %v", err), nil)
		}
		return
	}

	slog.Info("Manual backup completed successfully", "user", h.AuthenticatedUser, "file", fileName)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Manual backup completed successfully. Saved as '%s'.", fileName), nil)
	}
}

// HandleRestore maneja el comando para restaurar desde un backup.
// Implementamos un "Hot Restore" lógico: limpiamos los buckets físicos actuales y copiamos los del backup.
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

	backupName, err := protocol.ReadRestoreCommand(r)
	if err != nil {
		slog.Error("Failed to read RESTORE command payload", "remote_addr", remoteAddr, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid RESTORE command format.", nil)
		}
		return
	}
	if backupName == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Backup name cannot be empty.", nil)
		}
		return
	}

	backupPath := filepath.Join("backups", backupName)
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("ERROR: Backup file '%s' not found.", backupName), nil)
		return
	}

	slog.Warn("DESTRUCTIVE ACTION: Hot Restore initiated", "user", h.AuthenticatedUser, "backup_name", backupName, "remote_addr", remoteAddr)

	// 1. Abrir la base de datos de backup en modo Solo-Lectura
	backupDB, err := bbolt.Open(backupPath, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		slog.Error("Failed to open backup DB file", "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Could not read backup file. It might be corrupted.", nil)
		return
	}
	defer backupDB.Close()

	// 2. CORRECCIÓN: Borrar TODO el contenido físico de la base de datos actual.
	err = store.GlobalDB.Update(func(tx *bbolt.Tx) error {
		// Recopilamos los nombres de todos los buckets actuales
		var bucketsToDelete [][]byte
		tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			bucketsToDelete = append(bucketsToDelete, name)
			return nil
		})
		// Los eliminamos físicamente
		for _, name := range bucketsToDelete {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		slog.Error("Failed to wipe current database for restore", "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Could not wipe current database.", nil)
		return
	}

	// Limpiar referencias en la memoria RAM
	currentCollections := h.CollectionManager.ListCollections()
	for _, colName := range currentCollections {
		// Usamos un helper interno o simplemente ignoramos,
		// InitializeFromDisk reescribirá la RAM correctamente.
		h.CollectionManager.DeleteCollection(colName)
	}
	slog.Info("Live collections wiped successfully for restore.")

	// 3. Copiar todos los Buckets del Backup a la Base de Datos Viva
	err = backupDB.View(func(btx *bbolt.Tx) error {
		return store.GlobalDB.Update(func(gtx *bbolt.Tx) error {
			return btx.ForEach(func(name []byte, b *bbolt.Bucket) error {
				// Crear el bucket en la BD activa
				newB, err := gtx.CreateBucketIfNotExists(name)
				if err != nil {
					return err
				}
				// Copiar llave por llave
				return b.ForEach(func(k, v []byte) error {
					return newB.Put(k, v)
				})
			})
		})
	})

	if err != nil {
		slog.Error("Restore failed during data transfer", "backup_name", backupName, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Restore failed during data copy: %v", err), nil)
		}
		return
	}

	// 4. Reconstruir la RAM
	// Primero, obligamos al CollectionManager a registrar los Buckets recién restaurados
	// llamando a GetCollection (que los instancia en RAM si existen en disco).
	restoredNames := h.CollectionManager.ListCollections()
	for _, name := range restoredNames {
		h.CollectionManager.GetCollection(name)
	}

	// Ahora sí, poblamos los Árboles B de esos DiskStores
	if err := h.CollectionManager.InitializeFromDisk(); err != nil {
		slog.Error("Failed to rebuild indexes after restore", "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Data restored but index rebuild failed. Restart recommended.", nil)
		return
	}

	slog.Info("Restore completed successfully", "backup_name", backupName, "user", h.AuthenticatedUser)
	if conn != nil {
		msg := fmt.Sprintf("OK: Restore from '%s' completed successfully. All indexes are hot and ready.", backupName)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
	}
}
