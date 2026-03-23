package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// TestStdoutWriterBufferDrop tests that Write drops entries when the buffer is full.
func TestStdoutWriterBufferDrop(t *testing.T) {
	w := NewStdoutWriter(StdoutWriterConfig{
		FlushInterval: time.Hour,
		BatchSize:     100,
		BufferSize:    2,
	})

	w.Write(ProfileEntry{SRQueryID: "q1"})
	w.Write(ProfileEntry{SRQueryID: "q2"})
	w.Write(ProfileEntry{SRQueryID: "q3"}) // should be dropped

	if len(w.entries) != 2 {
		t.Errorf("expected 2 entries in buffer, got %d", len(w.entries))
	}
}

// TestStdoutWriterFlush tests that flush writes valid JSONL to stdout.
func TestStdoutWriterFlush(t *testing.T) {
	// Capture stdout by replacing os.Stdout with a pipe.
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w

	writer := &StdoutWriter{
		entries:       make(chan ProfileEntry, 100),
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

	writer.flush(batch)

	// Close the write end and read the captured output.
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 JSONL lines on stdout, got %d: %q", len(lines), output)
	}

	var decoded ProfileEntry
	if err := json.Unmarshal([]byte(lines[0]), &decoded); err != nil {
		t.Fatalf("failed to decode line 0: %v", err)
	}
	if decoded.SRQueryID != "q1" {
		t.Errorf("expected sr_query_id=q1, got %s", decoded.SRQueryID)
	}

	if err := json.Unmarshal([]byte(lines[1]), &decoded); err != nil {
		t.Fatalf("failed to decode line 1: %v", err)
	}
	if decoded.SRQueryID != "q2" {
		t.Errorf("expected sr_query_id=q2, got %s", decoded.SRQueryID)
	}
}
