package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"

	"lunadb/internal/config"
	"lunadb/internal/globalconst"
	"lunadb/internal/handler"
	"lunadb/internal/store"
)

var lastActivity atomic.Value

func init() {
	lastActivity.Store(time.Now())
}

type updateActivityFunc func()

func (f updateActivityFunc) UpdateActivity() {
	lastActivity.Store(time.Now())
}

func main() {
	// --- 1. Configuración e Inicialización de Logs ---
	if err := godotenv.Load(); err != nil {
		slog.Info("No .env file found, proceeding with existing environment")
	}

	if err := os.MkdirAll("logs", 0755); err != nil {
		slog.Error("Failed to create log directory", "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll("json", 0755); err != nil {
		slog.Error("Failed to create json directory", "error", err)
		os.Exit(1)
	}
	logFile, err := os.OpenFile("logs/lunadb.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		slog.Error("Failed to open log file", "error", err)
		os.Exit(1)
	}
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	slog.SetDefault(slog.New(slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelError, // Cambia a slog.LevelDebug para ver más detalles en desarrollo
	})))
	slog.Info("Logger configured successfully")

	cfg := config.LoadConfig()

	// --- 2. Inicialización del Motor de Disco (Disk-First) ---
	if err := os.MkdirAll("data", 0755); err != nil {
		slog.Error("Fatal: failed to create data directory", "error", err)
		os.Exit(1)
	}

	dbPath := filepath.Join("data", "luna.db")
	if err := store.InitDiskEngine(dbPath, cfg.NumShards); err != nil {
		slog.Error("Fatal error starting Disk Engine", "error", err)
		os.Exit(1)
	}
	// Nos aseguramos de que el motor en disco se cierre correctamente al salir
	defer store.CloseDiskEngine()

	// Inicializamos nuestros administradores
	collectionManager := store.NewCollectionManager()

	// Reconstruir B-Trees en RAM leyendo los Buckets del disco
	if err := collectionManager.InitializeFromDisk(); err != nil {
		slog.Error("Fatal error initializing collections from disk", "error", err)
		os.Exit(1)
	}

	transactionManager := store.NewTransactionManager(collectionManager)
	transactionManager.StartGC(5*time.Minute, 1*time.Minute)

	// --- 3. Creación de Usuarios por Defecto (ACID en disco directamente) ---
	systemCollection := collectionManager.GetCollection(globalconst.SystemCollectionName)

	if _, found := systemCollection.Get(globalconst.UserPrefix + "admin"); !found {
		slog.Info("Default admin user not found, creating...", "user", "admin")
		hashedPassword, _ := handler.HashPassword(cfg.DefaultAdminPassword)
		adminUserInfo := handler.UserInfo{
			Username:     "admin",
			PasswordHash: hashedPassword,
			IsRoot:       false,
			Permissions:  map[string]string{"*": globalconst.PermissionWrite, globalconst.SystemCollectionName: globalconst.PermissionRead},
		}
		adminUserInfoBytes, _ := bson.Marshal(adminUserInfo)
		// Ya no necesitamos EnqueueSaveTask, el Set va directo al disco físico
		systemCollection.Set(globalconst.UserPrefix+"admin", adminUserInfoBytes)
	}

	if _, found := systemCollection.Get(globalconst.UserPrefix + "root"); !found {
		slog.Info("Default root user not found, creating...", "user", "root")
		hashedPassword, _ := handler.HashPassword(cfg.DefaultRootPassword)
		rootUserInfo := handler.UserInfo{
			Username:     "root",
			PasswordHash: hashedPassword,
			IsRoot:       true,
			Permissions:  map[string]string{"*": globalconst.PermissionWrite},
		}
		rootUserInfoBytes, _ := bson.Marshal(rootUserInfo)
		systemCollection.Set(globalconst.UserPrefix+"root", rootUserInfoBytes)
	}

	// --- 4. Inicialización del Servidor TLS ---
	cert, err := tls.LoadX509KeyPair("certificates/server.crt", "certificates/server.key")
	if err != nil {
		slog.Error("Failed to load server certificate or key", "error", err)
		os.Exit(1)
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	listener, err := tls.Listen("tcp", cfg.Port, tlsConfig)
	if err != nil {
		slog.Error("Fatal error starting TLS TCP server", "port", cfg.Port, "error", err)
		os.Exit(1)
	}
	defer listener.Close()
	slog.Info("TLS TCP server listening securely", "port", cfg.Port)

	// --- 5. Worker Pool de Conexiones ---
	jobs := make(chan net.Conn, cfg.WorkerPoolSize)
	for w := 1; w <= cfg.WorkerPoolSize; w++ {
		go func(id int) {
			for conn := range jobs {
				h := handler.GetConnectionHandlerFromPool(
					collectionManager,
					transactionManager,
					updateActivityFunc(func() { lastActivity.Store(time.Now()) }),
					conn,
				)
				h.HandleConnection(conn)
				handler.PutConnectionHandlerToPool(h)
			}
		}(w)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" {
					slog.Info("Network listener closed, stopping connection acceptance.")
					close(jobs)
				} else {
					slog.Error("Error accepting connection", "error", err)
				}
				return
			}
			jobs <- conn
		}
	}()

	// --- 6. Tareas en Segundo Plano ---
	shutdownChan := make(chan struct{})

	// Worker para liberar memoria inactiva del SO (Relevante para el GC de Go)
	go func() {
		checkInterval := 2 * time.Minute
		idleThreshold := 5 * time.Minute
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		slog.Info("Starting idle memory cleaner", "check_interval", checkInterval.String(), "idle_threshold", idleThreshold.String())
		for {
			select {
			case <-ticker.C:
				lastActive := lastActivity.Load().(time.Time)
				if time.Since(lastActive) >= idleThreshold {
					slog.Info("Inactivity detected, requesting Go runtime to release OS memory...")
					debug.FreeOSMemory()
				}
			case <-shutdownChan:
				slog.Info("Idle memory cleaner stopped.")
				return
			}
		}
	}()

	// Worker de Backups Automáticos Periódicos (Soporte Sharding)
	go func() {
		ticker := time.NewTicker(cfg.BackupInterval)
		defer ticker.Stop()
		slog.Info("Starting automated sharded backup worker", "interval", cfg.BackupInterval.String())

		for {
			select {
			case <-ticker.C:
				baseBackupDir := "backups"
				os.MkdirAll(baseBackupDir, 0755)

				// 1. Crear la carpeta específica para este backup automático
				folderName := fmt.Sprintf("lunadb_auto_%s", time.Now().Format("20060102_150405"))
				backupDirPath := filepath.Join(baseBackupDir, folderName)

				if err := os.MkdirAll(backupDirPath, 0755); err != nil {
					slog.Error("Failed to create automated backup directory", "error", err)
					continue
				}

				var backupErr error

				// 2. Iterar sobre todos los shards y hacer un Hot Backup de cada uno
				for i := 0; i < store.TotalShards; i++ {
					shardFileName := fmt.Sprintf("shard_%d.db", i)
					shardFilePath := filepath.Join(backupDirPath, shardFileName)

					file, err := os.Create(shardFilePath)
					if err != nil {
						backupErr = fmt.Errorf("failed to create backup file for shard %d: %w", i, err)
						break
					}

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
					slog.Error("Automated backup failed", "error", backupErr)
					os.RemoveAll(backupDirPath) // Limpiar la carpeta corrupta/incompleta si algo falla
				} else {
					slog.Info("Automated sharded backup completed successfully", "folder", folderName)
				}

				// 3. Limpieza de backups viejos (ahora son carpetas, usamos RemoveAll)
				cutoffTime := time.Now().Add(-cfg.BackupRetention)
				if entries, err := os.ReadDir(baseBackupDir); err == nil {
					for _, entry := range entries {
						if info, err := entry.Info(); err == nil && info.ModTime().Before(cutoffTime) {
							dirToRemove := filepath.Join(baseBackupDir, entry.Name())
							os.RemoveAll(dirToRemove) // IMPORTANTE: RemoveAll borra la carpeta y su contenido
							slog.Info("Old backup deleted", "folder", entry.Name())
						}
					}
				}

			case <-shutdownChan:
				slog.Info("Automated backup worker stopped.")
				return
			}
		}
	}()

	// --- 7. Graceful Shutdown ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("🚀 ¡LunaDB Disk-First Engine initialized. ACID-compliant and ready to accept connections!")
	<-sigChan

	slog.Info("Termination signal received. Starting graceful shutdown...")

	if err := listener.Close(); err != nil {
		slog.Error("Error closing TCP listener", "error", err)
	} else {
		slog.Info("TCP listener closed.")
	}

	close(shutdownChan)
	transactionManager.StopGC()

	slog.Info("Application exiting cleanly.")
}
