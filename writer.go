// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// writer.go — Writer interface for profile entry output backends.
package main

// Writer defines the interface for profile entry output backends.
// Implementations buffer entries and flush them periodically or
// when a batch size threshold is reached.
type Writer interface {
	// Write queues a profile entry for output. Non-blocking; may drop
	// the entry if the internal buffer is full.
	Write(entry ProfileEntry)

	// Start begins the background flush goroutine.
	Start()

	// Close gracefully shuts down the writer, flushing remaining entries.
	Close() error
}
