package store

import (
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
)

// GlobalDB mantiene la conexión única al archivo físico.
var GlobalDB *bbolt.DB

// InitDiskEngine inicializa la base de datos bbolt.
// Debe ser llamado desde tu main.go al arrancar el servidor.
func InitDiskEngine(dbPath string) error {
	slog.Info("Initializing Disk Engine (bbolt)...", "path", dbPath)

	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to open bbolt database: %w", err)
	}

	GlobalDB = db
	return nil
}

// CloseDiskEngine cierra la base de datos de forma segura.
func CloseDiskEngine() error {
	if GlobalDB != nil {
		slog.Info("Closing Disk Engine...")
		return GlobalDB.Close()
	}
	return nil
}
