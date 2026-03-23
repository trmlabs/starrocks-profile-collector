// main.go — Entry point for the StarRocks profile collector. Loads configuration,
// initializes the selected storage backend, starts the metrics HTTP server and
// FE polling loop, and handles SIGINT/SIGTERM for graceful shutdown.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := loadConfig()

	if cfg.ClusterName == "" {
		slog.Error("CLUSTER_NAME is required")
		os.Exit(1)
	}
	if len(cfg.FEHosts) == 0 && cfg.FEHeadlessService == "" {
		slog.Error("Either FE_HOSTS or FE_HEADLESS_SERVICE is required")
		os.Exit(1)
	}

	slog.Info("configuration loaded",
		"cluster_name", cfg.ClusterName,
		"fe_hosts", cfg.FEHosts,
		"fe_user", cfg.FEUser,
		"poll_interval", cfg.PollInterval.String(),
		"storage_backend", cfg.StorageBackend,
		"flush_interval", cfg.FlushInterval.String(),
		"batch_size", cfg.BatchSize,
		"buffer_size", cfg.BufferSize,
		"dedup_ttl", cfg.DedupTTL.String(),
		"max_dedup_entries", cfg.MaxDedupEntries,
		"max_concurrent_fetches", cfg.MaxConcurrentFetches,
		"http_retries", cfg.HTTPRetries,
		"metrics_port", cfg.MetricsPort,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer, err := newWriter(ctx, cfg)
	if err != nil {
		slog.Error("failed to initialize storage backend", "backend", cfg.StorageBackend, "error", err)
		os.Exit(1)
	}
	writer.Start()

	ready := &atomic.Bool{}
	metricsServer := startMetricsServer(cfg.MetricsPort, ready)

	collector := NewCollector(cfg, writer)
	collector.ready = ready
	go collector.Run(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("received shutdown signal", "signal", sig.String())

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics server shutdown error", "error", err)
	}

	if err := writer.Close(); err != nil {
		slog.Error("writer close error", "error", err)
	}

	slog.Info("shutdown complete")
}

// newWriter creates a Writer based on the configured storage backend.
func newWriter(ctx context.Context, cfg *Config) (Writer, error) {
	switch cfg.StorageBackend {
	case "gcs":
		if cfg.GCSBucket == "" {
			return nil, fmt.Errorf("GCS_BUCKET is required when STORAGE_BACKEND=gcs")
		}
		return NewGCSWriter(ctx, GCSWriterConfig{
			GCSBucket:     cfg.GCSBucket,
			GCSPrefix:     cfg.GCSPrefix,
			FlushInterval: cfg.FlushInterval,
			BatchSize:     cfg.BatchSize,
			BufferSize:    cfg.BufferSize,
		})

	case "file":
		return NewFileWriter(FileWriterConfig{
			OutputDir:     cfg.OutputDir,
			Prefix:        cfg.FilePrefix,
			FlushInterval: cfg.FlushInterval,
			BatchSize:     cfg.BatchSize,
			BufferSize:    cfg.BufferSize,
		})

	case "stdout":
		return NewStdoutWriter(StdoutWriterConfig{
			FlushInterval: cfg.FlushInterval,
			BatchSize:     cfg.BatchSize,
			BufferSize:    cfg.BufferSize,
		}), nil

	default:
		return nil, fmt.Errorf("unknown STORAGE_BACKEND: %q (supported: gcs, file, stdout)", cfg.StorageBackend)
	}
}

// startMetricsServer starts the Prometheus metrics HTTP server with /health
// and /ready endpoints. Returns the *http.Server for graceful shutdown.
func startMetricsServer(addr string, ready *atomic.Bool) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "OK")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "NOT READY")
		}
	})

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("metrics server started", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()

	return server
}
