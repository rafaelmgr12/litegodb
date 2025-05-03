package config

import (
	"time"
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
