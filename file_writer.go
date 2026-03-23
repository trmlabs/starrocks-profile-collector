// file_writer.go — Batched local filesystem JSONL writer for profile entries.
// Writes Hive-style partitioned JSONL files to a local directory, using the
// same path structure as the GCS writer for compatibility with tools that
// read Hive-partitioned data (e.g., Spark, Trino, DuckDB).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

// FileWriterConfig holds configuration for the local filesystem writer.
type FileWriterConfig struct {
	OutputDir     string
	Prefix        string
	FlushInterval time.Duration
	BatchSize     int
	BufferSize    int
}

// FileWriter handles async batching and writing ProfileEntry records to the local filesystem.
type FileWriter struct {
	entries       chan ProfileEntry
	outputDir     string
	prefix        string
	flushInterval time.Duration
	batchSize     int

	cancel context.CancelFunc
	ctx    context.Context
	wg     sync.WaitGroup
}

// NewFileWriter creates a new FileWriter. Call Start() to begin the flush loop.
func NewFileWriter(cfg FileWriterConfig) (*FileWriter, error) {
	baseDir := filepath.Join(cfg.OutputDir, cfg.Prefix)
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory %s: %w", baseDir, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &FileWriter{
		entries:       make(chan ProfileEntry, cfg.BufferSize),
		outputDir:     cfg.OutputDir,
		prefix:        cfg.Prefix,
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Start begins the background flush goroutine.
func (w *FileWriter) Start() {
	w.wg.Add(1)
	go w.flushLoop()
	slog.Info("file writer started", "output_dir", w.outputDir, "prefix", w.prefix,
		"flush_interval", w.flushInterval.String(), "batch_size", w.batchSize)
}

// Write queues a profile entry for writing. Non-blocking; drops the entry if
// the buffer is full.
func (w *FileWriter) Write(entry ProfileEntry) {
	select {
	case w.entries <- entry:
	default:
		slog.Warn("file writer buffer full, dropping entry", "sr_query_id", entry.SRQueryID)
	}
}

// Close gracefully shuts down the writer, flushing remaining entries.
func (w *FileWriter) Close() error {
	slog.Info("file writer shutting down")
	w.cancel()
	w.wg.Wait()
	slog.Info("file writer shutdown complete")
	return nil
}

func (w *FileWriter) flushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batch := make([]ProfileEntry, 0, w.batchSize)

	for {
		select {
		case entry, ok := <-w.entries:
			if !ok {
				if len(batch) > 0 {
					w.flush(batch)
				}
				return
			}
			batch = append(batch, entry)
			if len(batch) >= w.batchSize {
				w.flush(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = batch[:0]
			}

		case <-w.ctx.Done():
			for {
				select {
				case entry := <-w.entries:
					batch = append(batch, entry)
				default:
					goto done
				}
			}
		done:
			if len(batch) > 0 {
				w.flush(batch)
			}
			return
		}
	}
}

func (w *FileWriter) flush(batch []ProfileEntry) {
	if len(batch) == 0 {
		return
	}

	flushStart := time.Now()

	now := time.Now().UTC()
	relPath := fmt.Sprintf("%s/year=%s/month=%s/day=%s/hour=%s",
		w.prefix,
		now.Format("2006"),
		now.Format("01"),
		now.Format("02"),
		now.Format("15"),
	)
	dir := filepath.Join(w.outputDir, relPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("failed to create partition directory", "error", err, "dir", dir)
		flushesTotal.WithLabelValues("error").Inc()
		return
	}

	filename := fmt.Sprintf("%s_%s.jsonl", now.Format("20060102_150405"), uuid.New().String()[:8])
	filePath := filepath.Join(dir, filename)

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, entry := range batch {
		if err := encoder.Encode(entry); err != nil {
			slog.Error("failed to encode profile entry", "error", err)
			continue
		}
	}

	if err := os.WriteFile(filePath, buf.Bytes(), 0644); err != nil {
		slog.Error("failed to write JSONL file", "error", err, "path", filePath)
		flushesTotal.WithLabelValues("error").Inc()
		return
	}

	flushDuration := time.Since(flushStart)
	bytesWritten := int64(buf.Len())
	flushesTotal.WithLabelValues("success").Inc()
	flushDurationSeconds.Observe(flushDuration.Seconds())
	entriesPerFlush.Observe(float64(len(batch)))
	bytesWrittenTotal.Add(float64(bytesWritten))
	lastFlushTimestamp.Set(float64(time.Now().Unix()))

	slog.Info("file flush complete", "entries", len(batch), "bytes", bytesWritten,
		"path", filePath, "duration", flushDuration.String())
}
