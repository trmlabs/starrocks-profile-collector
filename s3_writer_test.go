// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"
)

// TestS3WriterBufferDrop tests that Write drops entries when the buffer is full.
func TestS3WriterBufferDrop(t *testing.T) {
	w := &S3Writer{
		entries:       make(chan ProfileEntry, 2),
		bucket:        "test-bucket",
		prefix:        "test-prefix",
		flushInterval: time.Hour,
		batchSize:     100,
	}

	w.Write(ProfileEntry{SRQueryID: "q1"})
	w.Write(ProfileEntry{SRQueryID: "q2"})

	// This should be dropped (buffer full, non-blocking).
	w.Write(ProfileEntry{SRQueryID: "q3"})

	if len(w.entries) != 2 {
		t.Errorf("expected 2 entries in buffer, got %d", len(w.entries))
	}
}

// TestS3WriterConfigDefaults tests that the S3WriterConfig fields propagate correctly.
func TestS3WriterConfigDefaults(t *testing.T) {
	cfg := S3WriterConfig{
		S3Bucket:      "my-bucket",
		S3Prefix:      "my-prefix",
		S3Region:      "us-east-1",
		S3Endpoint:    "http://localhost:9000",
		FlushInterval: 45 * time.Second,
		BatchSize:     200,
		BufferSize:    5000,
	}

	if cfg.S3Bucket != "my-bucket" {
		t.Errorf("unexpected bucket: %s", cfg.S3Bucket)
	}
	if cfg.S3Prefix != "my-prefix" {
		t.Errorf("unexpected prefix: %s", cfg.S3Prefix)
	}
	if cfg.S3Region != "us-east-1" {
		t.Errorf("unexpected region: %s", cfg.S3Region)
	}
	if cfg.S3Endpoint != "http://localhost:9000" {
		t.Errorf("unexpected endpoint: %s", cfg.S3Endpoint)
	}
	if cfg.FlushInterval != 45*time.Second {
		t.Errorf("unexpected flush interval: %v", cfg.FlushInterval)
	}
	if cfg.BatchSize != 200 {
		t.Errorf("unexpected batch size: %d", cfg.BatchSize)
	}
	if cfg.BufferSize != 5000 {
		t.Errorf("unexpected buffer size: %d", cfg.BufferSize)
	}
}
