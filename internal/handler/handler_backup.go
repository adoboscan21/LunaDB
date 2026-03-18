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
// Realiza un volcado en caliente (Hot Backup) de TODAS las particiones físicas (shards).
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

	slog.Info("Manual sharded backup initiated", "user", h.AuthenticatedUser, "remote_addr", remoteAddr)

	// 1. Crear directorio base de backups si no existe
	baseBackupDir := "backups"
	if err := os.MkdirAll(baseBackupDir, 0755); err != nil {
		slog.Error("Failed to create backups directory", "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to create backups directory.", nil)
		}
		return
	}

	// 2. Crear una CARPETA específica para este backup (ya que ahora son múltiples archivos)
	backupName := fmt.Sprintf("lunadb_backup_%s", time.Now().Format("20060102_150405"))
	backupDirPath := filepath.Join(baseBackupDir, backupName)

	if err := os.MkdirAll(backupDirPath, 0755); err != nil {
		slog.Error("Failed to create specific backup directory", "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to create specific backup directory.", nil)
		}
		return
	}

	// 3. Volcado nativo de bbolt (Hot Backup) iterando sobre todos los Shards
	var backupErr error
	for i := 0; i < store.TotalShards; i++ {
		shardFileName := fmt.Sprintf("shard_%d.db", i)
		shardFilePath := filepath.Join(backupDirPath, shardFileName)

		file, err := os.Create(shardFilePath)
		if err != nil {
			backupErr = fmt.Errorf("failed to create backup file for shard %d: %w", i, err)
			break
		}

		// Copia en caliente directa desde el motor de la partición actual
		err = store.GlobalDBs[i].View(func(tx *bbolt.Tx) error {
			_, writeErr := tx.WriteTo(file)
			return writeErr
		})

		file.Close()

		if err != nil {
			backupErr = fmt.Errorf("failed to write backup for shard %d: %w", i, err)
			break
		}
	}

	if backupErr != nil {
		slog.Error("Manual backup failed", "user", h.AuthenticatedUser, "error", backupErr)
		os.RemoveAll(backupDirPath) // Limpiar la carpeta corrupta/incompleta
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Backup failed: %v", backupErr), nil)
		}
		return
	}

	slog.Info("Manual sharded backup completed successfully", "user", h.AuthenticatedUser, "folder", backupName)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Manual backup completed successfully. Saved in folder '%s'.", backupName), nil)
	}
}

// HandleRestore maneja el comando para restaurar desde un backup.
// Implementa un "Hot Restore" en todas las particiones simultáneamente.
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

	backupDirPath := filepath.Join("backups", backupName)
	info, err := os.Stat(backupDirPath)
	if os.IsNotExist(err) || !info.IsDir() {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("ERROR: Backup folder '%s' not found.", backupName), nil)
		}
		return
	}

	// Validación de seguridad: Validar que el backup tenga los mismos shards que la base de datos activa
	for i := 0; i < store.TotalShards; i++ {
		shardFilePath := filepath.Join(backupDirPath, fmt.Sprintf("shard_%d.db", i))
		if _, err := os.Stat(shardFilePath); os.IsNotExist(err) {
			errMsg := fmt.Errorf("backup folder does not contain 'shard_%d.db'. Ensure the backup was made with NumShards=%d", i, store.TotalShards)
			slog.Error("Restore validation failed", "error", errMsg)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: %v", errMsg), nil)
			}
			return
		}
	}

	slog.Warn("DESTRUCTIVE ACTION: Hot Sharded Restore initiated", "user", h.AuthenticatedUser, "backup_name", backupName, "remote_addr", remoteAddr)

	// 1. Borrar TODO el contenido físico de TODAS las particiones actuales.
	for i := 0; i < store.TotalShards; i++ {
		err = store.GlobalDBs[i].Update(func(tx *bbolt.Tx) error {
			var bucketsToDelete [][]byte
			tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
				bucketsToDelete = append(bucketsToDelete, name)
				return nil
			})
			for _, name := range bucketsToDelete {
				if err := tx.DeleteBucket(name); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			slog.Error("Failed to wipe current database shard for restore", "shard", i, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Could not wipe current database shards.", nil)
			}
			return
		}
	}

	// 2. Limpiar referencias en la memoria RAM
	currentCollections := h.CollectionManager.ListCollections()
	for _, colName := range currentCollections {
		h.CollectionManager.DeleteCollection(colName)
	}
	slog.Info("Live collections wiped successfully for restore.")

	// 3. Copiar todos los Buckets del Backup a la Base de Datos Viva, Shard por Shard
	for i := 0; i < store.TotalShards; i++ {
		shardFilePath := filepath.Join(backupDirPath, fmt.Sprintf("shard_%d.db", i))

		// Abrir la base de datos de backup en modo Solo-Lectura
		backupDB, err := bbolt.Open(shardFilePath, 0600, &bbolt.Options{ReadOnly: true})
		if err != nil {
			slog.Error("Failed to open backup shard DB file", "shard", i, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Could not read backup shard %d. Corrupted?", i), nil)
			}
			return
		}

		err = backupDB.View(func(btx *bbolt.Tx) error {
			return store.GlobalDBs[i].Update(func(gtx *bbolt.Tx) error {
				return btx.ForEach(func(name []byte, b *bbolt.Bucket) error {
					newB, err := gtx.CreateBucketIfNotExists(name)
					if err != nil {
						return err
					}
					return b.ForEach(func(k, v []byte) error {
						return newB.Put(k, v)
					})
				})
			})
		})

		backupDB.Close()

		if err != nil {
			slog.Error("Restore failed during data transfer", "shard", i, "backup_name", backupName, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Restore failed during data copy on shard %d: %v", i, err), nil)
			}
			return
		}
	}

	// 4. Reconstruir la RAM
	// Primero, instanciamos en RAM todo lo que acabamos de copiar a disco.
	// Nota: Ya no podemos usar ListCollections() si no los hemos registrado.
	// Haremos que InitializeFromDisk busque en los discos para levantar todo.

	if err := h.CollectionManager.InitializeFromDisk(); err != nil {
		slog.Error("Failed to rebuild indexes after restore", "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Data restored but index rebuild failed. Restart recommended.", nil)
		}
		return
	}

	slog.Info("Restore completed successfully", "backup_name", backupName, "user", h.AuthenticatedUser)
	if conn != nil {
		msg := fmt.Sprintf("OK: Restore from folder '%s' completed successfully. All shards and indexes are hot and ready.", backupName)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
	}
}
