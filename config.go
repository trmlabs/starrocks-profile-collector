// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// config.go — Configuration struct and environment variable loading for the
// StarRocks profile collector. All settings are read from environment variables
// with sensible defaults.
package main

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the collector configuration loaded from environment variables.
type Config struct {
	ClusterName       string   // Identifier for this StarRocks cluster (e.g. "my-cluster")
	FEHosts           []string // Static list of FE HTTP endpoints (e.g. ["fe-0:8030", "fe-1:8030"])
	FEHeadlessService string   // Headless K8s service for dynamic FE discovery
	FEHTTPPort        string   // FE HTTP port used with headless service discovery (default "8030")
	FEUser            string   // StarRocks HTTP API user
	FEPassword        string   // StarRocks HTTP API password
	PollInterval      time.Duration

	// Storage backend selection: "gcs", "s3", "file", or "stdout".
	StorageBackend string

	// GCS backend configuration.
	GCSBucket string
	GCSPrefix string

	// S3 backend configuration.
	S3Bucket   string
	S3Prefix   string
	S3Region   string
	S3Endpoint string // Custom endpoint for S3-compatible stores (e.g., MinIO)

	// File backend configuration.
	OutputDir  string // Base directory for local file output
	FilePrefix string // Path prefix within OutputDir

	// Common writer settings.
	FlushInterval time.Duration
	BatchSize     int
	BufferSize    int

	DedupTTL             time.Duration
	MaxDedupEntries      int
	MaxConcurrentFetches int
	HTTPRetries          int
	MetricsPort          string
	CheckpointDir        string
}

// loadConfig reads configuration from environment variables and returns a Config.
func loadConfig() *Config {
	feHostsRaw := getEnv("FE_HOSTS", "")
	var feHosts []string
	if feHostsRaw != "" {
		for _, h := range strings.Split(feHostsRaw, ",") {
			h = strings.TrimSpace(h)
			if h != "" {
				feHosts = append(feHosts, h)
			}
		}
	}

	return &Config{
		ClusterName:       getEnv("CLUSTER_NAME", ""),
		FEHosts:           feHosts,
		FEHeadlessService: getEnv("FE_HEADLESS_SERVICE", ""),
		FEHTTPPort:        getEnv("FE_HTTP_PORT", "8030"),
		FEUser:            getEnv("FE_USER", "root"),
		FEPassword:        getEnv("FE_PASSWORD", ""),
		PollInterval:      getEnvDuration("POLL_INTERVAL", 5*time.Second),

		StorageBackend: getEnv("STORAGE_BACKEND", "gcs"),

		GCSBucket: getEnv("GCS_BUCKET", ""),
		GCSPrefix: getEnv("GCS_PREFIX", "starrocks-query-profiles"),

		S3Bucket:   getEnv("S3_BUCKET", ""),
		S3Prefix:   getEnv("S3_PREFIX", "starrocks-query-profiles"),
		S3Region:   getEnv("S3_REGION", ""),
		S3Endpoint: getEnv("S3_ENDPOINT", ""),

		OutputDir:  getEnv("OUTPUT_DIR", "./output"),
		FilePrefix: getEnv("FILE_PREFIX", "starrocks-query-profiles"),

		FlushInterval: getEnvDuration("FLUSH_INTERVAL", 120*time.Second),
		BatchSize:     getEnvInt("BATCH_SIZE", 1000),
		BufferSize:    getEnvInt("BUFFER_SIZE", 10000),

		DedupTTL:             getEnvDuration("DEDUP_TTL", 30*time.Minute),
		MaxDedupEntries:      getEnvInt("MAX_DEDUP_ENTRIES", 100000),
		MaxConcurrentFetches: getEnvInt("MAX_CONCURRENT_FETCHES", 10),
		HTTPRetries:          getEnvInt("HTTP_RETRIES", 1),
		MetricsPort:          getEnv("METRICS_PORT", ":9091"),
		CheckpointDir:        getEnv("CHECKPOINT_DIR", ""),
	}
}

// getEnv returns the value of an environment variable, or the default if unset.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt returns an integer environment variable, or the default if unset or invalid.
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
		slog.Warn("invalid integer value for env var, using default", "key", key, "value", value, "default", defaultValue)
	}
	return defaultValue
}

// getEnvDuration parses a duration from an environment variable (e.g. "5s", "30m").
// Falls back to the default if unset or unparseable.
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
		slog.Warn("invalid duration for env var, using default", "key", key, "value", value, "default", defaultValue.String())
	}
	return defaultValue
}
