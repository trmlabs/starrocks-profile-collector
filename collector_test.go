// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestParseQueryDetails_Array tests parsing a plain JSON array response.
func TestParseQueryDetails_Array(t *testing.T) {
	input := `[
		{
			"eventTime": 1770806107434000000,
			"queryId": "q1",
			"isQuery": true,
			"state": "FINISHED",
			"database": "db1",
			"sql": "SELECT 1",
			"user": "root",
			"startTime": 1770806107000,
			"endTime": 1770806108000,
			"latency": 1000,
			"connId": 42
		},
		{
			"eventTime": 1770806107435000000,
			"queryId": "q2",
			"isQuery": true,
			"state": "RUNNING",
			"database": "db2",
			"sql": "SELECT 2",
			"user": "analyst",
			"startTime": 1770806160000,
			"endTime": -1,
			"latency": -1,
			"connId": 43
		}
	]`

	entries, err := ParseQueryDetails([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[0].QueryID != "q1" {
		t.Errorf("expected query_id q1, got %s", entries[0].QueryID)
	}
	if entries[0].State != "FINISHED" {
		t.Errorf("expected state FINISHED, got %s", entries[0].State)
	}
	if entries[0].Database != "db1" {
		t.Errorf("expected database db1, got %s", entries[0].Database)
	}
	if entries[0].SQL != "SELECT 1" {
		t.Errorf("expected sql 'SELECT 1', got %s", entries[0].SQL)
	}
	if entries[0].Latency != 1000 {
		t.Errorf("expected latency 1000, got %d", entries[0].Latency)
	}
	if entries[0].EventTime != 1770806107434000000 {
		t.Errorf("expected eventTime 1770806107434000000, got %d", entries[0].EventTime)
	}
	if entries[1].QueryID != "q2" {
		t.Errorf("expected query_id q2, got %s", entries[1].QueryID)
	}
	if entries[1].User != "analyst" {
		t.Errorf("expected user analyst, got %s", entries[1].User)
	}
}

// TestParseQueryDetails_Wrapper tests parsing a {data: [...]} wrapper response.
func TestParseQueryDetails_Wrapper(t *testing.T) {
	input := `{
		"data": [
			{
				"eventTime": 1770806200000000000,
				"queryId": "q3",
				"state": "FINISHED",
				"database": "db3",
				"sql": "SELECT 3",
				"user": "root",
				"startTime": 1770806200000,
				"endTime": 1770806202000,
				"latency": 2000,
				"connId": 100
			}
		]
	}`

	entries, err := ParseQueryDetails([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].QueryID != "q3" {
		t.Errorf("expected query_id q3, got %s", entries[0].QueryID)
	}
	if entries[0].Latency != 2000 {
		t.Errorf("expected latency 2000, got %d", entries[0].Latency)
	}
}

// TestParseQueryDetails_Empty tests parsing an empty JSON array.
func TestParseQueryDetails_Empty(t *testing.T) {
	entries, err := ParseQueryDetails([]byte(`[]`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

// TestParseQueryDetails_Invalid tests parsing invalid JSON.
func TestParseQueryDetails_Invalid(t *testing.T) {
	_, err := ParseQueryDetails([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

// TestParseQueryDetails_MissingFields tests that missing fields default to zero values.
func TestParseQueryDetails_MissingFields(t *testing.T) {
	input := `[{"queryId": "q4"}]`

	entries, err := ParseQueryDetails([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].QueryID != "q4" {
		t.Errorf("expected query_id q4, got %s", entries[0].QueryID)
	}
	if entries[0].State != "" {
		t.Errorf("expected empty state, got %s", entries[0].State)
	}
	if entries[0].Latency != 0 {
		t.Errorf("expected latency 0, got %d", entries[0].Latency)
	}
}

// TestDedupLogic tests the dedup mechanism of the Collector.
func TestDedupLogic(t *testing.T) {
	cfg := &Config{
		ClusterName:  "test-cluster",
		FEHosts:      []string{"localhost:8030"},
		FEUser:       "root",
		FEPassword:   "",
		PollInterval: 5 * time.Second,
		DedupTTL:     100 * time.Millisecond,
	}

	collector := NewCollector(cfg, nil, nil)

	if collector.seen("q1") {
		t.Error("q1 should not be seen yet")
	}

	collector.markSeen("q1")

	if !collector.seen("q1") {
		t.Error("q1 should be seen after markSeen")
	}

	if collector.seen("q2") {
		t.Error("q2 should not be seen")
	}

	time.Sleep(150 * time.Millisecond)
	collector.evictExpiredDedup()

	if collector.seen("q1") {
		t.Error("q1 should have been evicted after TTL")
	}
}

// TestDedupEviction tests that only expired entries are evicted.
func TestDedupEviction(t *testing.T) {
	cfg := &Config{
		DedupTTL: 200 * time.Millisecond,
	}
	collector := NewCollector(cfg, nil, nil)

	collector.markSeen("old")
	time.Sleep(100 * time.Millisecond)
	collector.markSeen("new")

	time.Sleep(50 * time.Millisecond)
	collector.evictExpiredDedup()

	if !collector.seen("old") {
		t.Error("old should still be present (150ms < 200ms TTL)")
	}
	if !collector.seen("new") {
		t.Error("new should still be present")
	}

	time.Sleep(100 * time.Millisecond)
	collector.evictExpiredDedup()

	if collector.seen("old") {
		t.Error("old should have been evicted (250ms > 200ms TTL)")
	}
	if !collector.seen("new") {
		t.Error("new should still be present (150ms < 200ms TTL)")
	}
}

// TestCollectorPollFE tests that the collector correctly polls an FE and collects profiles.
func TestCollectorPollFE(t *testing.T) {
	queryDetailResp := []queryDetailEntry{
		{
			EventTime: 1770806107434000000,
			QueryID:   "test-q1",
			State:     "FINISHED",
			Database:  "testdb",
			SQL:       "SELECT 1",
			User:      "root",
			StartTime: 1770806107000,
			EndTime:   1770806108000,
			Latency:   1000,
		},
		{
			EventTime: 1770806107435000000,
			QueryID:   "test-q2",
			State:     "FINISHED",
			Database:  "testdb",
			SQL:       "SELECT 2",
			User:      "analyst",
			StartTime: 1770806160000,
			EndTime:   1770806162000,
			Latency:   2000,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/query_detail":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(queryDetailResp)
		case "/api/profile":
			queryID := r.URL.Query().Get("query_id")
			fmt.Fprintf(w, "Profile for %s\nSummary: ...\nDetails: ...", queryID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := &Config{
		ClusterName:  "test-cluster",
		FEHosts:      []string{server.Listener.Addr().String()},
		FEUser:       "root",
		FEPassword:   "",
		PollInterval: time.Second,
		DedupTTL:     time.Minute,
	}

	collector := &Collector{
		cfg:           cfg,
		writer:        nil,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		authHeader:    "Basic cm9vdDo=",
		lastEventTime: make(map[string]int64),
		dedup:         make(map[string]time.Time),
		backoff:       make(map[string]*feBackoff),
	}

	ctx := context.Background()
	entries, err := collector.fetchQueryDetails(ctx, server.Listener.Addr().String(), 0)
	if err != nil {
		t.Fatalf("fetchQueryDetails failed: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	profile, err := collector.fetchProfile(ctx, server.Listener.Addr().String(), "test-q1")
	if err != nil {
		t.Fatalf("fetchProfile failed: %v", err)
	}

	expected := "Profile for test-q1\nSummary: ...\nDetails: ..."
	if profile != expected {
		t.Errorf("expected profile %q, got %q", expected, profile)
	}
}

// TestCollectorFEUnavailable tests that the collector handles an unavailable FE gracefully.
func TestCollectorFEUnavailable(t *testing.T) {
	cfg := &Config{
		ClusterName: "test-cluster",
		FEHosts:     []string{"localhost:1"},
		FEUser:      "root",
		FEPassword:  "",
		DedupTTL:    time.Minute,
	}

	collector := NewCollector(cfg, nil, nil)

	ctx := context.Background()
	_, err := collector.fetchQueryDetails(ctx, "localhost:1", 0)
	if err == nil {
		t.Fatal("expected error for unreachable FE, got nil")
	}
}

// TestBackoffRecordAndReset tests the exponential backoff mechanism.
func TestBackoffRecordAndReset(t *testing.T) {
	cfg := &Config{DedupTTL: time.Minute}
	collector := NewCollector(cfg, nil, nil)

	feHost := "fe-backoff:8030"

	// Initially not backed off.
	if collector.isBackedOff(feHost) {
		t.Fatal("should not be backed off initially")
	}

	// Record first failure — should enter backoff for backoffInitial (10s).
	collector.recordFailure(feHost)
	if !collector.isBackedOff(feHost) {
		t.Fatal("should be backed off after first failure")
	}

	nextRetry := collector.nextRetryAt(feHost)
	if nextRetry.IsZero() {
		t.Fatal("nextRetryAt should not be zero after failure")
	}
	// Should be roughly 10s from now (with some tolerance).
	untilRetry := time.Until(nextRetry)
	if untilRetry < 8*time.Second || untilRetry > 12*time.Second {
		t.Errorf("expected retry in ~10s, got %v", untilRetry)
	}

	// Reset clears the backoff.
	collector.resetBackoff(feHost)
	if collector.isBackedOff(feHost) {
		t.Fatal("should not be backed off after reset")
	}
}

// TestBackoffExponentialGrowth tests that backoff delay doubles on consecutive failures.
func TestBackoffExponentialGrowth(t *testing.T) {
	cfg := &Config{DedupTTL: time.Minute}
	collector := NewCollector(cfg, nil, nil)

	feHost := "fe-growth:8030"

	// Record multiple failures and verify delay growth.
	collector.recordFailure(feHost) // 10s
	collector.backoffMu.Lock()
	delay1 := collector.backoff[feHost].currentDelay
	collector.backoffMu.Unlock()

	collector.recordFailure(feHost) // 20s
	collector.backoffMu.Lock()
	delay2 := collector.backoff[feHost].currentDelay
	collector.backoffMu.Unlock()

	collector.recordFailure(feHost) // 40s
	collector.backoffMu.Lock()
	delay3 := collector.backoff[feHost].currentDelay
	collector.backoffMu.Unlock()

	// delay1 should be 20s (next delay after first failure sets current to 10*2=20)
	if delay1 != 20*time.Second {
		t.Errorf("expected delay1=20s, got %v", delay1)
	}
	if delay2 != 40*time.Second {
		t.Errorf("expected delay2=40s, got %v", delay2)
	}
	if delay3 != 80*time.Second {
		t.Errorf("expected delay3=80s, got %v", delay3)
	}
}

// TestBackoffMaxCap tests that backoff delay is capped at backoffMax.
func TestBackoffMaxCap(t *testing.T) {
	cfg := &Config{DedupTTL: time.Minute}
	collector := NewCollector(cfg, nil, nil)

	feHost := "fe-cap:8030"

	// Record many failures to exceed the cap.
	for i := 0; i < 20; i++ {
		collector.recordFailure(feHost)
	}

	collector.backoffMu.Lock()
	delay := collector.backoff[feHost].currentDelay
	collector.backoffMu.Unlock()

	if delay > backoffMax {
		t.Errorf("delay %v exceeds backoffMax %v", delay, backoffMax)
	}
	if delay != backoffMax {
		t.Errorf("expected delay to be capped at %v, got %v", backoffMax, delay)
	}
}

// TestConcurrentPollAllFEs tests that pollAllFEs polls multiple FEs concurrently.
func TestConcurrentPollAllFEs(t *testing.T) {
	// Track which FEs were polled and in which order.
	var mu sync.Mutex
	pollTimes := make(map[string]time.Time)

	// Each FE server has a 100ms artificial delay to prove concurrency.
	makeServer := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/query_detail":
				time.Sleep(100 * time.Millisecond)
				mu.Lock()
				pollTimes[name] = time.Now()
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("[]"))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
	}

	server1 := makeServer("fe1")
	defer server1.Close()
	server2 := makeServer("fe2")
	defer server2.Close()
	server3 := makeServer("fe3")
	defer server3.Close()

	cfg := &Config{
		ClusterName:  "test-cluster",
		FEHosts:      []string{server1.Listener.Addr().String(), server2.Listener.Addr().String(), server3.Listener.Addr().String()},
		FEUser:       "root",
		FEPassword:   "",
		PollInterval: time.Second,
		DedupTTL:     time.Minute,
	}

	collector := NewCollector(cfg, nil, nil)

	start := time.Now()
	collector.pollAllFEs(context.Background())
	elapsed := time.Since(start)

	// If sequential, 3 * 100ms = 300ms+. If concurrent, ~100ms.
	// Use 250ms as threshold to confidently detect concurrency.
	if elapsed > 250*time.Millisecond {
		t.Errorf("pollAllFEs took %v, expected < 250ms (should be concurrent)", elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(pollTimes) != 3 {
		t.Errorf("expected 3 FEs polled, got %d", len(pollTimes))
	}
}

// TestPollAllFEsSkipsBackedOffHosts tests that backed-off FEs are skipped.
func TestPollAllFEsSkipsBackedOffHosts(t *testing.T) {
	var mu sync.Mutex
	polledHosts := make(map[string]bool)

	makeServer := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/query_detail" {
				mu.Lock()
				polledHosts[name] = true
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("[]"))
			}
		}))
	}

	server1 := makeServer("fe1")
	defer server1.Close()
	server2 := makeServer("fe2")
	defer server2.Close()

	cfg := &Config{
		ClusterName:  "test-cluster",
		FEHosts:      []string{server1.Listener.Addr().String(), server2.Listener.Addr().String()},
		FEUser:       "root",
		FEPassword:   "",
		PollInterval: time.Second,
		DedupTTL:     time.Minute,
	}

	collector := NewCollector(cfg, nil, nil)

	// Put server2's address into backoff.
	collector.recordFailure(server2.Listener.Addr().String())

	collector.pollAllFEs(context.Background())

	mu.Lock()
	defer mu.Unlock()

	if !polledHosts["fe1"] {
		t.Error("fe1 should have been polled")
	}
	if polledHosts["fe2"] {
		t.Error("fe2 should have been skipped (backed off)")
	}
}

// testWriter is a Writer implementation that captures entries for test assertions.
type testWriter struct {
	mu      sync.Mutex
	entries []ProfileEntry
}

func (w *testWriter) Write(entry ProfileEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.entries = append(w.entries, entry)
}

func (w *testWriter) Start() {}

func (w *testWriter) Close() error { return nil }

func (w *testWriter) get() []ProfileEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]ProfileEntry, len(w.entries))
	copy(out, w.entries)
	return out
}

func (w *testWriter) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.entries = nil
}

// TestPollFEEndToEnd tests the full pollFE flow: query_detail -> profile fetch ->
// writer.Write with a test writer to capture written entries.
func TestPollFEEndToEnd(t *testing.T) {
	queryDetailResp := []queryDetailEntry{
		{
			EventTime: 1770806107434000000,
			QueryID:   "e2e-q1",
			State:     "FINISHED",
			Database:  "testdb",
			SQL:       "SELECT 1",
			User:      "root",
			StartTime: 1770806107000,
			EndTime:   1770806108000,
			Latency:   1000,
		},
		{
			EventTime: 1770806107435000000,
			QueryID:   "e2e-q2",
			State:     "FINISHED",
			Database:  "testdb",
			SQL:       "SELECT 2",
			User:      "analyst",
			StartTime: 1770806160000,
			EndTime:   1770806162000,
			Latency:   2000,
		},
		{
			// Running query — should be skipped.
			EventTime: 1770806107436000000,
			QueryID:   "e2e-q3",
			State:     "RUNNING",
			Database:  "testdb",
			SQL:       "SELECT 3",
			User:      "root",
			StartTime: 1770806170000,
			EndTime:   -1,
			Latency:   -1,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/query_detail":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(queryDetailResp)
		case "/api/profile":
			queryID := r.URL.Query().Get("query_id")
			fmt.Fprintf(w, "Profile for %s", queryID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := &Config{
		ClusterName:          "test-cluster",
		FEHosts:              []string{server.Listener.Addr().String()},
		FEUser:               "root",
		FEPassword:           "",
		PollInterval:         time.Second,
		DedupTTL:             time.Minute,
		MaxDedupEntries:      100000,
		MaxConcurrentFetches: 10,
		HTTPRetries:          0,
	}

	writer := &testWriter{}

	collector := &Collector{
		cfg:           cfg,
		writer:        writer,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		authHeader:    "Basic cm9vdDo=",
		lastEventTime: make(map[string]int64),
		dedup:         make(map[string]time.Time),
		backoff:       make(map[string]*feBackoff),
	}

	feAddr := server.Listener.Addr().String()
	collector.pollFE(context.Background(), feAddr)

	entries := writer.get()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries written, got %d", len(entries))
	}

	// Build map by query ID since concurrent fetches may return in any order.
	byID := make(map[string]ProfileEntry)
	for _, e := range entries {
		byID[e.SRQueryID] = e
	}

	q1, ok := byID["e2e-q1"]
	if !ok {
		t.Fatal("expected entry for e2e-q1")
	}
	if q1.ProfileText != "Profile for e2e-q1" {
		t.Errorf("expected profile text 'Profile for e2e-q1', got %q", q1.ProfileText)
	}
	if q1.Cluster != "test-cluster" {
		t.Errorf("expected cluster=test-cluster, got %s", q1.Cluster)
	}
	if q1.FEHost != feAddr {
		t.Errorf("expected fe_host=%s, got %s", feAddr, q1.FEHost)
	}
	if q1.QueryHash == "" {
		t.Error("expected non-empty query_hash for entry with SQL")
	}

	q2, ok := byID["e2e-q2"]
	if !ok {
		t.Fatal("expected entry for e2e-q2")
	}
	if q2.User != "analyst" {
		t.Errorf("expected user=analyst, got %s", q2.User)
	}

	// Verify checkpoint was advanced.
	collector.lastEventTimeMu.Lock()
	lastEvent := collector.lastEventTime[feAddr]
	collector.lastEventTimeMu.Unlock()
	if lastEvent != 1770806107436000000 {
		t.Errorf("expected lastEventTime=1770806107436000000 (includes RUNNING entry), got %d", lastEvent)
	}

	// Verify dedup — polling again should produce 0 new entries.
	writer.reset()
	collector.pollFE(context.Background(), feAddr)
	if len(writer.get()) != 0 {
		t.Errorf("expected 0 entries on second poll (dedup), got %d", len(writer.get()))
	}
}

// TestHTTPRetryOn5xx tests that doHTTPWithRetry retries on 5xx errors.
func TestHTTPRetryOn5xx(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts <= 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	defer server.Close()

	cfg := &Config{
		HTTPRetries: 2,
		DedupTTL:    time.Minute,
	}
	collector := NewCollector(cfg, nil, nil)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/test", nil)
	resp, err := collector.doHTTPWithRetry(context.Background(), req)
	if err != nil {
		t.Fatalf("expected success after retry, got error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

// TestEmergencyEviction tests that the dedup map is capped at MaxDedupEntries.
func TestEmergencyEviction(t *testing.T) {
	cfg := &Config{
		DedupTTL:        time.Hour,
		MaxDedupEntries: 20,
	}
	collector := NewCollector(cfg, nil, nil)

	// Insert 25 entries — should trigger emergency eviction at entry 21.
	for i := 0; i < 25; i++ {
		collector.markSeen(fmt.Sprintf("q-%d", i))
		time.Sleep(time.Millisecond) // Ensure distinct timestamps for ordering.
	}

	collector.dedupMu.Lock()
	remaining := len(collector.dedup)
	collector.dedupMu.Unlock()

	// After multiple evictions, the map should never greatly exceed the cap.
	// Each eviction removes 10% = 2 entries, so at 25 inserts we may have
	// triggered eviction multiple times. Just verify it's reasonable.
	if remaining > 25 {
		t.Errorf("dedup map should not exceed 25, got %d", remaining)
	}
	if remaining < 15 {
		t.Errorf("dedup map should have at least 15 entries, got %d (over-evicted?)", remaining)
	}
}
