package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config holds application-wide configuration.
type Config struct {
	Port                 string
	ShutdownTimeout      time.Duration
	BackupInterval       time.Duration
	BackupRetention      time.Duration
	WorkerPoolSize       int
	NumShards            int
	DefaultRootPassword  string
	DefaultAdminPassword string
}

// NewDefaultConfig creates a Config struct with sensible default values.
func NewDefaultConfig() Config {
	return Config{
		Port:                 ":5876",
		ShutdownTimeout:      10 * time.Second,
		BackupInterval:       1 * time.Hour,
		BackupRetention:      7 * 24 * time.Hour,
		WorkerPoolSize:       100,
		NumShards:            16,
		DefaultRootPassword:  "rootpass",
		DefaultAdminPassword: "adminpass",
	}
}

// LoadConfig loads configuration with a clear precedence: Environment > Defaults.
func LoadConfig() Config {
	cfg := NewDefaultConfig()
	slog.Info("Loading configuration...")
	applyEnvConfig(&cfg)
	return cfg
}

// applyEnvConfig overrides config values from environment variables.
func applyEnvConfig(cfg *Config) {
	if portEnv := os.Getenv("LUNADB_PORT"); portEnv != "" {
		cfg.Port = portEnv
		slog.Info("Overriding Port from environment", "value", portEnv)
	}

	if numShardsEnv := os.Getenv("LUNADB_NUM_SHARDS"); numShardsEnv != "" {
		if i, err := strconv.Atoi(numShardsEnv); err == nil && i > 0 {
			cfg.NumShards = i
		}
	}

	if rootPassEnv := os.Getenv("LUNADB_ROOT_PASSWORD"); rootPassEnv != "" {
		cfg.DefaultRootPassword = rootPassEnv
	}

	if adminPassEnv := os.Getenv("LUNADB_ADMIN_PASSWORD"); adminPassEnv != "" {
		cfg.DefaultAdminPassword = adminPassEnv
	}

	if workerPoolEnv := os.Getenv("LUNADB_WORKER_POOL_SIZE"); workerPoolEnv != "" {
		if i, err := strconv.Atoi(workerPoolEnv); err == nil && i > 0 {
			cfg.WorkerPoolSize = i
		}
	}

	overrideDuration("LUNADB_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)
	overrideDuration("LUNADB_BACKUP_INTERVAL", &cfg.BackupInterval)
	overrideDuration("LUNADB_BACKUP_RETENTION", &cfg.BackupRetention)
}

func overrideDuration(envKey string, target *time.Duration) {
	envVal := os.Getenv(envKey)
	if envVal != "" {
		if d, err := time.ParseDuration(envVal); err == nil {
			*target = d
			slog.Info("Overriding duration from environment", "key", envKey, "value", envVal)
		} else {
			slog.Warn("Invalid duration format in env var", "key", envKey, "value", envVal)
		}
	}
}
