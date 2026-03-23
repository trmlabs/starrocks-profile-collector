// stdout_writer.go — JSONL writer that outputs profile entries to stdout.
// Useful for piping output to other tools or for local debugging.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"
)

// StdoutWriter writes ProfileEntry records as JSONL to stdout.
type StdoutWriter struct {
	entries       chan ProfileEntry
	flushInterval time.Duration
	batchSize     int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// StdoutWriterConfig holds configuration for the stdout writer.
type StdoutWriterConfig struct {
	FlushInterval time.Duration
	BatchSize     int
	BufferSize    int
}

// NewStdoutWriter creates a new StdoutWriter.
func NewStdoutWriter(cfg StdoutWriterConfig) *StdoutWriter {
	ctx, cancel := context.WithCancel(context.Background())
	return &StdoutWriter{
		entries:       make(chan ProfileEntry, cfg.BufferSize),
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start begins the background flush goroutine.
func (w *StdoutWriter) Start() {
	w.wg.Add(1)
	go w.flushLoop()
	slog.Info("stdout writer started",
		"flush_interval", w.flushInterval.String(), "batch_size", w.batchSize)
}

// Write queues a profile entry for writing.
func (w *StdoutWriter) Write(entry ProfileEntry) {
	select {
	case w.entries <- entry:
	default:
		slog.Warn("stdout writer buffer full, dropping entry", "sr_query_id", entry.SRQueryID)
	}
}

// Close gracefully shuts down the writer, flushing remaining entries.
func (w *StdoutWriter) Close() error {
	slog.Info("stdout writer shutting down")
	w.cancel()
	w.wg.Wait()
	slog.Info("stdout writer shutdown complete")
	return nil
}

func (w *StdoutWriter) flushLoop() {
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

func (w *StdoutWriter) flush(batch []ProfileEntry) {
	if len(batch) == 0 {
		return
	}

	flushStart := time.Now()
	var bytesWritten int

	for _, entry := range batch {
		data, err := json.Marshal(entry)
		if err != nil {
			slog.Error("failed to encode profile entry", "error", err)
			continue
		}
		data = append(data, '\n')
		if _, err := os.Stdout.Write(data); err != nil {
			slog.Error("failed to write to stdout", "error", err)
			continue
		}
		bytesWritten += len(data)
	}

	flushDuration := time.Since(flushStart)
	flushesTotal.WithLabelValues("success").Inc()
	flushDurationSeconds.Observe(flushDuration.Seconds())
	entriesPerFlush.Observe(float64(len(batch)))
	bytesWrittenTotal.Add(float64(bytesWritten))
	lastFlushTimestamp.Set(float64(time.Now().Unix()))

	slog.Info("stdout flush complete", "entries", len(batch), "bytes", bytesWritten,
		"duration", flushDuration.String())
}
