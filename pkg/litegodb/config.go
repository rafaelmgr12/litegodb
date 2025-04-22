package litegodb

import (
	"fmt"
	"time"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
	"github.com/spf13/viper"
)

type Config struct {
	Degree     int           `mapstructure:"degree"`
	DBFile     string        `mapstructure:"db_file"`
	LogFile    string        `mapstructure:"log_file"`
	FlushEvery time.Duration `mapstructure:"flush_every"`
}

func Open(configPath string) (DB, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	dm, err := disk.NewFileDiskManager(cfg.DBFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	store, err := kvstore.NewBTreeKVStore(cfg.Degree, dm, cfg.LogFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	store.StartPeriodicFlush(cfg.FlushEvery)

	return &btreeAdapter{kv: store}, nil
}

func loadConfig(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	viper.SetDefault("degree", 2)
	viper.SetDefault("db_file", "data.db")
	viper.SetDefault("log_file", "wal.log")
	viper.SetDefault("flush_every", "10s")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("⚠️ Config file não encontrado, usando valores padrão.")
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
