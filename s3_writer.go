// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// s3_writer.go — Batched S3 JSONL writer for profile entries.
// Buffers entries in a channel and flushes to S3 as newline-delimited JSON
// on a timer or when the batch size threshold is reached. Uses the same
// Hive-style partitioned path structure as the GCS writer.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// S3WriterConfig holds configuration for the S3 writer.
type S3WriterConfig struct {
	S3Bucket      string
	S3Prefix      string
	S3Region      string
	S3Endpoint    string
	FlushInterval time.Duration
	BatchSize     int
	BufferSize    int
}

// S3Writer handles async batching and writing ProfileEntry records to S3.
type S3Writer struct {
	entries       chan ProfileEntry
	bucket        string
	prefix        string
	flushInterval time.Duration
	batchSize     int

	client *s3.Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewS3Writer creates a new S3 writer. Call Start() to begin the flush loop.
func NewS3Writer(ctx context.Context, cfg S3WriterConfig) (*S3Writer, error) {
	writerCtx, cancel := context.WithCancel(ctx)

	var opts []func(*config.LoadOptions) error
	if cfg.S3Region != "" {
		opts = append(opts, config.WithRegion(cfg.S3Region))
	}

	awsCfg, err := config.LoadDefaultConfig(writerCtx, opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.S3Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	// Verify bucket access early.
	_, err = client.HeadBucket(writerCtx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.S3Bucket),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to access S3 bucket %s: %w", cfg.S3Bucket, err)
	}

	return &S3Writer{
		entries:       make(chan ProfileEntry, cfg.BufferSize),
		bucket:        cfg.S3Bucket,
		prefix:        cfg.S3Prefix,
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		client:        client,
		ctx:           writerCtx,
		cancel:        cancel,
	}, nil
}

// Start begins the background flush goroutine.
func (w *S3Writer) Start() {
	w.wg.Add(1)
	go w.flushLoop()
	slog.Info("S3 writer started", "bucket", w.bucket, "prefix", w.prefix,
		"flush_interval", w.flushInterval.String(), "batch_size", w.batchSize)
}

// Write queues a profile entry for writing. Non-blocking; drops the entry if
// the buffer is full.
func (w *S3Writer) Write(entry ProfileEntry) {
	select {
	case w.entries <- entry:
	default:
		slog.Warn("S3 writer buffer full, dropping entry", "sr_query_id", entry.SRQueryID)
	}
}

// Close gracefully shuts down the writer, flushing remaining entries.
func (w *S3Writer) Close() error {
	slog.Info("S3 writer shutting down")
	w.cancel()
	w.wg.Wait()
	slog.Info("S3 writer shutdown complete")
	return nil
}

func (w *S3Writer) flushLoop() {
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

func (w *S3Writer) flush(batch []ProfileEntry) {
	if len(batch) == 0 {
		return
	}

	flushStart := time.Now()

	now := time.Now().UTC()
	objectKey := fmt.Sprintf("%s/year=%s/month=%s/day=%s/hour=%s/%s_%s.jsonl",
		w.prefix,
		now.Format("2006"),
		now.Format("01"),
		now.Format("02"),
		now.Format("15"),
		now.Format("20060102_150405"),
		uuid.New().String()[:8],
	)

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, entry := range batch {
		if err := encoder.Encode(entry); err != nil {
			slog.Error("failed to encode profile entry", "error", err)
			continue
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(w.bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/x-ndjson"),
	})
	if err != nil {
		slog.Error("failed to write to S3", "error", err, "key", objectKey, "entries", len(batch))
		flushesTotal.WithLabelValues("error").Inc()
		flushDurationSeconds.Observe(time.Since(flushStart).Seconds())
		w.fallbackLog(batch)
		return
	}

	flushDuration := time.Since(flushStart)
	bytesWritten := int64(buf.Len())
	flushesTotal.WithLabelValues("success").Inc()
	flushDurationSeconds.Observe(flushDuration.Seconds())
	entriesPerFlush.Observe(float64(len(batch)))
	bytesWrittenTotal.Add(float64(bytesWritten))
	lastFlushTimestamp.Set(float64(time.Now().Unix()))

	slog.Info("S3 flush complete", "entries", len(batch), "bytes", bytesWritten,
		"key", fmt.Sprintf("s3://%s/%s", w.bucket, objectKey), "duration", flushDuration.String())
}

func (w *S3Writer) fallbackLog(batch []ProfileEntry) {
	fallbackWritesTotal.Add(float64(len(batch)))
	for _, entry := range batch {
		data, err := json.Marshal(entry)
		if err != nil {
			slog.Error("fallback: failed to marshal entry", "error", err)
			continue
		}
		slog.Warn("fallback: writing entry to stdout", "sr_query_id", entry.SRQueryID, "entry", string(data))
	}
}
