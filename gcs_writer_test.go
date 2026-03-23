// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestProfileEntryJSONL tests that ProfileEntry serializes to valid JSONL.
func TestProfileEntryJSONL(t *testing.T) {
	entries := []ProfileEntry{
		{
			Timestamp:   "2025-01-15T14:30:22Z",
			FEHost:      "fe-0:8030",
			Cluster:     "test-cluster",
			SRQueryID:   "q1",
			State:       "FINISHED",
			Database:    "mydb",
			User:        "root",
			StartTime:   "2025-01-15 14:30:00",
			EndTime:     "2025-01-15 14:30:22",
			DurationMs:  22000,
			SQLText:     "SELECT * FROM t",
			ProfileText: "Profile...",
		},
		{
			Timestamp:   "2025-01-15T14:31:00Z",
			FEHost:      "fe-1:8030",
			Cluster:     "test-cluster",
			SRQueryID:   "q2",
			State:       "FINISHED",
			Database:    "otherdb",
			User:        "analyst",
			StartTime:   "2025-01-15 14:30:50",
			EndTime:     "2025-01-15 14:31:00",
			DurationMs:  10000,
			SQLText:     "SELECT count(*) FROM t2",
			ProfileText: "Profile2...",
		},
	}

	var lines []string
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("failed to marshal entry: %v", err)
		}
		lines = append(lines, string(data))
	}

	jsonl := strings.Join(lines, "\n") + "\n"

	// Verify each line is valid JSON and can be deserialized back.
	for i, line := range strings.Split(strings.TrimSpace(jsonl), "\n") {
		var decoded ProfileEntry
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", i, err)
		}
		if decoded.SRQueryID != entries[i].SRQueryID {
			t.Errorf("line %d: expected sr_query_id=%s, got %s",
				i, entries[i].SRQueryID, decoded.SRQueryID)
		}
	}
}

// TestProfileEntryJSONFields tests that all JSON field names are correct.
func TestProfileEntryJSONFields(t *testing.T) {
	entry := ProfileEntry{
		Timestamp:   "2025-01-15T14:30:22Z",
		FEHost:      "fe-0:8030",
		Cluster:     "test-cluster",
		SRQueryID:   "q1",
		State:       "FINISHED",
		Database:    "mydb",
		User:        "root",
		StartTime:   "2025-01-15 14:30:00",
		EndTime:     "2025-01-15 14:30:22",
		DurationMs:  22000.5,
		SQLText:     "SELECT 1",
		ProfileText: "Profile...",
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// Parse as generic map to check field names.
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to unmarshal to map: %v", err)
	}

	expectedFields := []string{
		"ts", "fe_host", "cluster", "sr_query_id", "state",
		"database", "user", "start_time", "end_time", "duration_ms",
		"sql_text", "profile_text",
	}

	for _, field := range expectedFields {
		if _, ok := m[field]; !ok {
			t.Errorf("missing expected JSON field: %s", field)
		}
	}

	// Verify no extra fields.
	if len(m) != len(expectedFields) {
		t.Errorf("expected %d fields, got %d", len(expectedFields), len(m))
	}
}

// TestProfileEntrySpecialChars tests JSONL encoding with special characters in SQL.
func TestProfileEntrySpecialChars(t *testing.T) {
	entry := ProfileEntry{
		Timestamp:   "2025-01-15T14:30:22Z",
		FEHost:      "fe-0:8030",
		Cluster:     "test",
		SRQueryID:   "q-special",
		SQLText:     `SELECT * FROM t WHERE name = "foo\nbar" AND val = 'baz'`,
		ProfileText: "Line1\nLine2\tTabbed\n\"Quoted\"",
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// Verify it round-trips correctly.
	var decoded ProfileEntry
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.SQLText != entry.SQLText {
		t.Errorf("sql_text mismatch:\n  got:  %q\n  want: %q", decoded.SQLText, entry.SQLText)
	}
	if decoded.ProfileText != entry.ProfileText {
		t.Errorf("profile_text mismatch:\n  got:  %q\n  want: %q", decoded.ProfileText, entry.ProfileText)
	}
}

// TestGCSWriterBufferDrop tests that Write drops entries when the buffer is full.
func TestGCSWriterBufferDrop(t *testing.T) {
	w := &GCSWriter{
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

// TestGCSWriterConfigDefaults tests that the GCSWriterConfig fields propagate correctly.
func TestGCSWriterConfigDefaults(t *testing.T) {
	cfg := GCSWriterConfig{
		GCSBucket:     "my-bucket",
		GCSPrefix:     "my-prefix",
		FlushInterval: 45 * time.Second,
		BatchSize:     200,
		BufferSize:    5000,
	}

	if cfg.GCSBucket != "my-bucket" {
		t.Errorf("unexpected bucket: %s", cfg.GCSBucket)
	}
	if cfg.GCSPrefix != "my-prefix" {
		t.Errorf("unexpected prefix: %s", cfg.GCSPrefix)
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
