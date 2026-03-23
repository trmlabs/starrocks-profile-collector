package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFileWriterBufferDrop tests that Write drops entries when the buffer is full.
func TestFileWriterBufferDrop(t *testing.T) {
	w := &FileWriter{
		entries:       make(chan ProfileEntry, 2),
		outputDir:     t.TempDir(),
		prefix:        "test",
		flushInterval: time.Hour,
		batchSize:     100,
	}

	w.Write(ProfileEntry{SRQueryID: "q1"})
	w.Write(ProfileEntry{SRQueryID: "q2"})
	w.Write(ProfileEntry{SRQueryID: "q3"}) // should be dropped

	if len(w.entries) != 2 {
		t.Errorf("expected 2 entries in buffer, got %d", len(w.entries))
	}
}

// TestFileWriterFlush tests that flush writes Hive-partitioned JSONL to disk.
func TestFileWriterFlush(t *testing.T) {
	dir := t.TempDir()
	w := &FileWriter{
		entries:       make(chan ProfileEntry, 100),
		outputDir:     dir,
		prefix:        "profiles",
		flushInterval: time.Hour,
		batchSize:     100,
	}

	batch := []ProfileEntry{
		{
			Timestamp: "2025-01-15T14:30:22Z",
			FEHost:    "fe-0:8030",
			Cluster:   "test",
			SRQueryID: "q1",
			State:     "FINISHED",
			SQLText:   "SELECT 1",
		},
		{
			Timestamp: "2025-01-15T14:31:00Z",
			FEHost:    "fe-1:8030",
			Cluster:   "test",
			SRQueryID: "q2",
			State:     "FINISHED",
			SQLText:   "SELECT 2",
		},
	}

	w.flush(batch)

	// Verify Hive-partitioned directory structure was created.
	now := time.Now().UTC()
	expectedDir := filepath.Join(dir, "profiles",
		"year="+now.Format("2006"),
		"month="+now.Format("01"),
		"day="+now.Format("02"),
		"hour="+now.Format("15"),
	)

	entries, err := os.ReadDir(expectedDir)
	if err != nil {
		t.Fatalf("expected partition directory %s to exist: %v", expectedDir, err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 JSONL file, got %d", len(entries))
	}

	filename := entries[0].Name()
	if !strings.HasSuffix(filename, ".jsonl") {
		t.Errorf("expected .jsonl extension, got %s", filename)
	}

	// Read and verify JSONL content.
	data, err := os.ReadFile(filepath.Join(expectedDir, filename))
	if err != nil {
		t.Fatalf("failed to read JSONL file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 JSONL lines, got %d", len(lines))
	}

	var decoded ProfileEntry
	if err := json.Unmarshal([]byte(lines[0]), &decoded); err != nil {
		t.Fatalf("failed to decode line 0: %v", err)
	}
	if decoded.SRQueryID != "q1" {
		t.Errorf("expected sr_query_id=q1, got %s", decoded.SRQueryID)
	}
	if decoded.SQLText != "SELECT 1" {
		t.Errorf("expected sql_text='SELECT 1', got %s", decoded.SQLText)
	}
}

// TestNewFileWriter tests that NewFileWriter creates the output directory.
func TestNewFileWriter(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "output")

	w, err := NewFileWriter(FileWriterConfig{
		OutputDir:     dir,
		Prefix:        "profiles",
		FlushInterval: time.Minute,
		BatchSize:     100,
		BufferSize:    100,
	})
	if err != nil {
		t.Fatalf("NewFileWriter failed: %v", err)
	}
	defer w.Close()

	expectedDir := filepath.Join(dir, "profiles")
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Errorf("expected directory %s to be created", expectedDir)
	}
}
