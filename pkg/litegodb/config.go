// Package litegodb provides configuration management and initialization
// for the lightweight key-value database.
package litegodb

import (
	"fmt"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
	"github.com/spf13/viper"
)

// Config represents the configuration for the database.
// It includes parameters for the B-Tree degree, file paths, and flush interval.
type Config struct {
	Degree     int           `mapstructure:"degree"`      // Degree of the B-Tree.
	DBFile     string        `mapstructure:"db_file"`     // Path to the database file.
	LogFile    string        `mapstructure:"log_file"`    // Path to the write-ahead log file.
	FlushEvery time.Duration `mapstructure:"flush_every"` // Interval for periodic flushes.
	Server     ServerConfig  `mapstructure:"server"`      // Server configuration.
}

type ServerConfig struct {
	Port       int    `mapstructure:"port"`
	EnableCORS bool   `mapstructure:"enable_cors"`
	AuthToken  string `mapstructure:"auth_token"`
}

// Open initializes and returns a new database instance based on the provided configuration file.
// It sets up the disk manager, B-Tree key-value store, and periodic flush mechanism.
func Open(configPath string) (DB, *Config, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load config: %w", err)
	}

	dm, err := disk.NewFileDiskManager(cfg.DBFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	store, err := kvstore.NewBTreeKVStore(cfg.Degree, dm, cfg.LogFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create store: %w", err)
	}

	store.StartPeriodicFlush(cfg.FlushEvery)

	return &btreeAdapter{kv: store}, cfg, nil
}

// loadConfig reads and parses the configuration file from the specified path.
// If the file is not found, it uses default values.
func loadConfig(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	// Default DB settings
	viper.SetDefault("degree", 2)
	viper.SetDefault("db_file", "data.db")
	viper.SetDefault("log_file", "wal.log")
	viper.SetDefault("flush_every", "10s")

	// Default Server settings
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.enable_cors", false)
	viper.SetDefault("server.auth_token", "")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("⚠️ Config file not found, using default values")
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
