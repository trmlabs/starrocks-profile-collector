// gcs_writer.go — Batched GCS JSONL writer for profile entries.
// Buffers entries in a channel and flushes to GCS as newline-delimited JSON
// on a timer or when the batch size threshold is reached.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

// GCSWriterConfig holds configuration for the GCS writer.
type GCSWriterConfig struct {
	GCSBucket     string        // GCS bucket name (without gs://)
	GCSPrefix     string        // Path prefix within bucket
	FlushInterval time.Duration // How often to flush (e.g. 30s)
	BatchSize     int           // Max entries before forced flush
	BufferSize    int           // Channel buffer size
}

// GCSWriter handles async batching and writing ProfileEntry records to GCS.
type GCSWriter struct {
	entries       chan ProfileEntry
	bucket        string
	prefix        string
	flushInterval time.Duration
	batchSize     int

	client *storage.Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewGCSWriter creates a new GCS writer. Call Start() to begin the flush loop.
func NewGCSWriter(ctx context.Context, cfg GCSWriterConfig) (*GCSWriter, error) {
	writerCtx, cancel := context.WithCancel(ctx)

	client, err := storage.NewClient(writerCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Verify bucket access early so misconfigurations are caught at startup.
	bucket := client.Bucket(cfg.GCSBucket)
	if _, err := bucket.Attrs(writerCtx); err != nil {
		cancel()
		client.Close()
		return nil, fmt.Errorf("failed to access GCS bucket %s: %w", cfg.GCSBucket, err)
	}

	return &GCSWriter{
		entries:       make(chan ProfileEntry, cfg.BufferSize),
		bucket:        cfg.GCSBucket,
		prefix:        cfg.GCSPrefix,
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		client:        client,
		ctx:           writerCtx,
		cancel:        cancel,
	}, nil
}

// Start begins the background flush goroutine.
func (w *GCSWriter) Start() {
	w.wg.Add(1)
	go w.flushLoop()
	slog.Info("GCS writer started", "bucket", w.bucket, "prefix", w.prefix,
		"flush_interval", w.flushInterval.String(), "batch_size", w.batchSize)
}

// Write queues a profile entry for writing. Non-blocking; drops the entry if
// the buffer is full and logs a warning.
func (w *GCSWriter) Write(entry ProfileEntry) {
	select {
	case w.entries <- entry:
		// Successfully queued.
	default:
		slog.Warn("GCS writer buffer full, dropping entry", "sr_query_id", entry.SRQueryID)
	}
}

// Close gracefully shuts down the writer, flushing remaining entries.
func (w *GCSWriter) Close() error {
	slog.Info("GCS writer shutting down")
	w.cancel()
	w.wg.Wait()
	slog.Info("GCS writer shutdown complete")
	return w.client.Close()
}

// flushLoop runs in the background, batching entries and writing to GCS.
func (w *GCSWriter) flushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batch := make([]ProfileEntry, 0, w.batchSize)

	for {
		select {
		case entry, ok := <-w.entries:
			if !ok {
				// Channel closed — flush remaining and exit.
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
			// Shutdown requested — drain the channel and flush.
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

// flush writes a batch of entries to GCS as JSONL (newline-delimited JSON).
func (w *GCSWriter) flush(batch []ProfileEntry) {
	if len(batch) == 0 {
		return
	}

	flushStart := time.Now()

	// Hive-style partitioned path for BigQuery partition pruning.
	now := time.Now().UTC()
	objectPath := fmt.Sprintf("%s/year=%s/month=%s/day=%s/hour=%s/%s_%s.jsonl",
		w.prefix,
		now.Format("2006"),
		now.Format("01"),
		now.Format("02"),
		now.Format("15"),
		now.Format("20060102_150405"),
		uuid.New().String()[:8],
	)

	// Encode batch to JSONL.
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, entry := range batch {
		if err := encoder.Encode(entry); err != nil {
			slog.Error("failed to encode profile entry", "error", err)
			continue
		}
	}
	bytesToWrite := int64(buf.Len())

	// Use a standalone timeout instead of deriving from w.ctx so that the
	// final drain flush during shutdown is not immediately cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	obj := w.client.Bucket(w.bucket).Object(objectPath)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/x-ndjson"

	if _, err := io.Copy(writer, &buf); err != nil {
		slog.Error("failed to write to GCS", "error", err, "path", objectPath, "entries", len(batch))
		writer.Close()
		gcsFlushesTotal.WithLabelValues("error").Inc()
		gcsFlushDuration.Observe(time.Since(flushStart).Seconds())
		w.fallbackLog(batch)
		return
	}

	if err := writer.Close(); err != nil {
		slog.Error("failed to close GCS writer", "error", err, "path", objectPath)
		gcsFlushesTotal.WithLabelValues("error").Inc()
		gcsFlushDuration.Observe(time.Since(flushStart).Seconds())
		w.fallbackLog(batch)
		return
	}

	// Record successful flush metrics.
	flushDuration := time.Since(flushStart)
	gcsFlushesTotal.WithLabelValues("success").Inc()
	gcsFlushDuration.Observe(flushDuration.Seconds())
	gcsEntriesPerFlush.Observe(float64(len(batch)))
	gcsBytesWritten.Add(float64(bytesToWrite))
	gcsLastFlushTimestamp.Set(float64(time.Now().Unix()))

	slog.Info("GCS flush complete", "entries", len(batch), "bytes", bytesToWrite,
		"path", fmt.Sprintf("gs://%s/%s", w.bucket, objectPath), "duration", flushDuration.String())
}

// fallbackLog writes entries to stdout as JSON when GCS writes fail.
func (w *GCSWriter) fallbackLog(batch []ProfileEntry) {
	gcsFallbackWrites.Add(float64(len(batch)))
	for _, entry := range batch {
		data, err := json.Marshal(entry)
		if err != nil {
			slog.Error("fallback: failed to marshal entry", "error", err)
			continue
		}
		slog.Warn("fallback: writing entry to stdout", "sr_query_id", entry.SRQueryID, "entry", string(data))
	}
}
