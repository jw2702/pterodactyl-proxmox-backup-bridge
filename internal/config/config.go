// Package config loads and validates the bridge's configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ListenAddr string

	AccessKey string
	SecretKey string
	Region    string
	ClockSkew time.Duration

	DataDir    string
	ScratchDir string

	MultipartTTL time.Duration
	GCInterval   time.Duration

	PBSRepository     string
	PBSPassword       string
	PBSAPIToken       string
	PBSFingerprint    string
	PBSBackupType     string
	PBSCommandTimeout time.Duration
	PBSBinPath        string

	LogLevel  string
	LogFormat string
}

// Load reads configuration from the environment and validates required fields.
// It fails fast with a descriptive error if anything required is missing or malformed.
func Load() (Config, error) {
	cfg := Config{
		ListenAddr: getEnv("BRIDGE_LISTEN_ADDR", ":8080"),

		AccessKey: os.Getenv("BRIDGE_ACCESS_KEY"),
		SecretKey: os.Getenv("BRIDGE_SECRET_KEY"),
		Region:    getEnv("BRIDGE_REGION", "us-east-1"),

		DataDir:    getEnv("BRIDGE_DATA_DIR", "/var/lib/bridge/data"),
		ScratchDir: getEnv("BRIDGE_SCRATCH_DIR", "/var/lib/bridge/scratch"),

		PBSRepository:  os.Getenv("PBS_REPOSITORY"),
		PBSPassword:    os.Getenv("PBS_PASSWORD"),
		PBSAPIToken:    os.Getenv("PBS_API_TOKEN"),
		PBSFingerprint: os.Getenv("PBS_FINGERPRINT"),
		PBSBackupType:  getEnv("BRIDGE_PBS_BACKUP_TYPE", "host"),
		PBSBinPath:     getEnv("BRIDGE_PBS_BIN_PATH", "proxmox-backup-client"),

		LogLevel:  getEnv("BRIDGE_LOG_LEVEL", "info"),
		LogFormat: getEnv("BRIDGE_LOG_FORMAT", "json"),
	}

	var err error
	cfg.ClockSkew, err = getEnvSeconds("BRIDGE_CLOCK_SKEW_SECONDS", 900)
	if err != nil {
		return cfg, err
	}
	cfg.MultipartTTL, err = getEnvHours("BRIDGE_MULTIPART_TTL_HOURS", 24)
	if err != nil {
		return cfg, err
	}
	cfg.GCInterval, err = getEnvMinutes("BRIDGE_GC_INTERVAL_MINUTES", 30)
	if err != nil {
		return cfg, err
	}
	cfg.PBSCommandTimeout, err = getEnvSeconds("BRIDGE_PBS_COMMAND_TIMEOUT_SECONDS", 3600)
	if err != nil {
		return cfg, err
	}

	if err := cfg.validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func (c Config) validate() error {
	var missing []string
	if c.AccessKey == "" {
		missing = append(missing, "BRIDGE_ACCESS_KEY")
	}
	if c.SecretKey == "" {
		missing = append(missing, "BRIDGE_SECRET_KEY")
	}
	if c.PBSRepository == "" {
		missing = append(missing, "PBS_REPOSITORY")
	}
	if len(missing) > 0 {
		return fmt.Errorf("config: missing required environment variables: %v", missing)
	}
	if c.PBSPassword == "" && c.PBSAPIToken == "" {
		return fmt.Errorf("config: either PBS_PASSWORD or PBS_API_TOKEN must be set")
	}
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvSeconds(key string, def int) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(def) * time.Second, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s: %w", key, err)
	}
	return time.Duration(n) * time.Second, nil
}

func getEnvMinutes(key string, def int) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(def) * time.Minute, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s: %w", key, err)
	}
	return time.Duration(n) * time.Minute, nil
}

func getEnvHours(key string, def int) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(def) * time.Hour, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s: %w", key, err)
	}
	return time.Duration(n) * time.Hour, nil
}
