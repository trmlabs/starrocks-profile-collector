// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// collector.go — Polls StarRocks FE nodes for query details and fetches full
// execution profiles via HTTP. Deduplicates across FE nodes and polling cycles.
package main

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"sort"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// errProfileNotFound is returned by fetchProfile when the FE returns 404.
// This is a permanent error -- the profile was never collected or has been evicted.
var errProfileNotFound = errors.New("profile not found")

// ProfileEntry represents a single collected query profile for GCS/BigQuery.
type ProfileEntry struct {
	Timestamp   string  `json:"ts"`
	FEHost      string  `json:"fe_host"`
	Cluster     string  `json:"cluster"`
	SRQueryID   string  `json:"sr_query_id"`
	State       string  `json:"state"`
	Database    string  `json:"database"`
	User        string  `json:"user"`
	StartTime   string  `json:"start_time"`
	EndTime     string  `json:"end_time"`
	DurationMs  float64 `json:"duration_ms"`
	SQLText     string  `json:"sql_text"`
	QueryHash   string  `json:"query_hash,omitempty"` // MD5 hash of SQL text for cross-system correlation
	ProfileText string  `json:"profile_text"`
}

// queryDetailEntry represents a single entry from the /api/query_detail response.
// The API returns numeric timestamps (epoch millis) and latency in ms.
type queryDetailEntry struct {
	EventTime int64  `json:"eventTime"` // Nanosecond timestamp for pagination
	QueryID   string `json:"queryId"`
	IsQuery   bool   `json:"isQuery"`
	State     string `json:"state"`
	Database  string `json:"database"`
	SQL       string `json:"sql"`
	User      string `json:"user"`
	StartTime int64  `json:"startTime"` // Epoch millis
	EndTime   int64  `json:"endTime"`   // Epoch millis (-1 if running)
	Latency   int64  `json:"latency"`   // Milliseconds (-1 if running)
	ConnID    int64  `json:"connId"`
}

// Maximum response body sizes to prevent OOM on unexpectedly large responses.
const (
	maxQueryDetailResponseBytes = 50 << 20 // 50 MB for query detail listing
	maxProfileResponseBytes    = 10 << 20  // 10 MB per individual profile
)

// Backoff parameters for FE hosts that are repeatedly failing.
const (
	backoffInitial = 10 * time.Second
	backoffMax     = 5 * time.Minute
	backoffFactor  = 2.0
)

// feBackoff tracks exponential backoff state for a single FE host.
type feBackoff struct {
	failures     int
	nextRetryAt  time.Time
	currentDelay time.Duration
}

// Collector polls StarRocks FE HTTP APIs for query details and profiles.
type Collector struct {
	cfg        *Config
	writer     Writer
	httpClient *http.Client
	authHeader string

	// lastEventTime tracks the highest eventTime seen per FE for pagination.
	lastEventTimeMu sync.Mutex
	lastEventTime   map[string]int64

	// dedup tracks seen query IDs to avoid re-fetching profiles.
	dedupMu sync.RWMutex
	dedup   map[string]time.Time

	// backoff tracks per-FE exponential backoff on repeated failures.
	backoffMu sync.Mutex
	backoff   map[string]*feBackoff

	// ready is set to true after the first successful FE poll. Used by the
	// /ready health endpoint to signal Kubernetes readiness.
	ready *atomic.Bool
}

// NewCollector creates a new Collector. The ready flag is set to true after the
// first successful FE poll; pass nil to disable readiness signaling.
func NewCollector(cfg *Config, writer Writer, ready *atomic.Bool) *Collector {
	credentials := base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s:%s", cfg.FEUser, cfg.FEPassword)),
	)

	return &Collector{
		cfg:    cfg,
		writer: writer,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		authHeader:    "Basic " + credentials,
		lastEventTime: make(map[string]int64),
		dedup:         make(map[string]time.Time),
		backoff:       make(map[string]*feBackoff),
		ready:         ready,
	}
}

// loadCheckpoint restores lastEventTime from a local file if CheckpointDir is configured.
func (c *Collector) loadCheckpoint() {
	if c.cfg.CheckpointDir == "" {
		return
	}
	path := filepath.Join(c.cfg.CheckpointDir, "checkpoint.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("no checkpoint file found, starting fresh", "path", path)
		} else {
			slog.Warn("failed to read checkpoint", "path", path, "error", err)
		}
		return
	}
	var checkpoint map[string]int64
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		slog.Warn("failed to parse checkpoint", "path", path, "error", err)
		return
	}
	c.lastEventTimeMu.Lock()
	c.lastEventTime = checkpoint
	c.lastEventTimeMu.Unlock()
	slog.Info("restored checkpoint", "fe_entries", len(checkpoint), "path", path)
}

// saveCheckpoint persists lastEventTime to a local file.
func (c *Collector) saveCheckpoint() {
	if c.cfg.CheckpointDir == "" {
		return
	}
	c.lastEventTimeMu.Lock()
	data, err := json.Marshal(c.lastEventTime)
	c.lastEventTimeMu.Unlock()
	if err != nil {
		slog.Error("failed to marshal checkpoint", "error", err)
		return
	}
	if err := os.MkdirAll(c.cfg.CheckpointDir, 0755); err != nil {
		slog.Error("failed to create checkpoint dir", "error", err)
		return
	}
	path := filepath.Join(c.cfg.CheckpointDir, "checkpoint.json")
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		slog.Error("failed to write checkpoint temp file", "path", tmpPath, "error", err)
		return
	}
	if err := os.Rename(tmpPath, path); err != nil {
		slog.Error("failed to rename checkpoint temp file", "from", tmpPath, "to", path, "error", err)
	}
}

// Run starts the polling loop and blocks until the context is cancelled.
func (c *Collector) Run(ctx context.Context) {
	collectorActive.Set(1)
	defer collectorActive.Set(0)

	// Restore checkpoint from previous run.
	c.loadCheckpoint()

	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	if c.cfg.FEHeadlessService != "" {
		slog.Info("collector started with dynamic FE discovery",
			"headless_service", c.cfg.FEHeadlessService, "http_port", c.cfg.FEHTTPPort,
			"poll_interval", c.cfg.PollInterval.String())
	} else {
		slog.Info("collector started with static FE list",
			"fe_hosts", len(c.cfg.FEHosts), "poll_interval", c.cfg.PollInterval.String())
	}

	// Initial poll immediately.
	c.pollAllFEs(ctx)

	for {
		select {
		case <-ticker.C:
			c.pollAllFEs(ctx)
			c.saveCheckpoint()
			c.evictExpiredDedup()
		case <-ctx.Done():
			c.saveCheckpoint()
			slog.Info("collector stopping")
			return
		}
	}
}

// resolveFEHosts returns the list of FE hosts to poll. If FEHeadlessService is
// configured, it resolves the headless service DNS to discover all FE pod IPs
// dynamically. Otherwise it returns the static FEHosts list from config.
func (c *Collector) resolveFEHosts() []string {
	if c.cfg.FEHeadlessService == "" {
		return c.cfg.FEHosts
	}

	ips, err := net.LookupHost(c.cfg.FEHeadlessService)
	if err != nil {
		slog.Error("failed to resolve FE headless service, falling back to static list",
			"service", c.cfg.FEHeadlessService, "error", err, "fallback_count", len(c.cfg.FEHosts))
		return c.cfg.FEHosts
	}

	hosts := make([]string, len(ips))
	for i, ip := range ips {
		hosts[i] = fmt.Sprintf("%s:%s", ip, c.cfg.FEHTTPPort)
	}
	slog.Debug("resolved FE hosts from headless service", "service", c.cfg.FEHeadlessService, "count", len(hosts))
	return hosts
}

// pollAllFEs polls every configured FE host concurrently. Each FE is polled
// in its own goroutine so that one slow or unreachable FE does not block the
// others. The dedup map is already mutex-protected, so concurrent access is safe.
func (c *Collector) pollAllFEs(ctx context.Context) {
	feHosts := c.resolveFEHosts()
	var wg sync.WaitGroup
	for _, feHost := range feHosts {
		if ctx.Err() != nil {
			return
		}
		// Skip FEs that are in a backoff window.
		if c.isBackedOff(feHost) {
			slog.Debug("skipping FE in backoff", "fe_host", feHost,
				"next_retry_at", c.nextRetryAt(feHost).Format(time.RFC3339))
			continue
		}
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			c.pollFE(ctx, host)
		}(feHost)
	}
	wg.Wait()
}

// pollFE polls a single FE host for query details and fetches new profiles.
func (c *Collector) pollFE(ctx context.Context, feHost string) {
	start := time.Now()
	pollsTotal.WithLabelValues(feHost).Inc()

	// Get the last event_time for this FE for incremental polling.
	c.lastEventTimeMu.Lock()
	eventTime := c.lastEventTime[feHost]
	c.lastEventTimeMu.Unlock()

	entries, err := c.fetchQueryDetails(ctx, feHost, eventTime)
	if err != nil {
		c.recordFailure(feHost)
		slog.Error("failed to fetch query_detail", "fe_host", feHost, "error", err)
		pollDurationSeconds.WithLabelValues(feHost).Observe(time.Since(start).Seconds())
		return
	}
	c.resetBackoff(feHost)

	// Mark as ready after the first successful FE poll so the /ready
	// endpoint starts returning 200.
	if c.ready != nil {
		c.ready.Store(true)
	}

	var maxProcessedEventTime int64

	// Semaphore to bound concurrent profile fetches per FE.
	sem := make(chan struct{}, c.cfg.MaxConcurrentFetches)
	var fetchWg sync.WaitGroup

	// Collect results from concurrent profile fetches.
	type fetchResult struct {
		entry       queryDetailEntry
		profileText string
	}
	resultsCh := make(chan fetchResult, len(entries))

	for _, entry := range entries {
		if ctx.Err() != nil {
			break
		}

		if entry.QueryID == "" || entry.State != "FINISHED" {
			if entry.EventTime > maxProcessedEventTime {
				maxProcessedEventTime = entry.EventTime
			}
			continue
		}

		if c.seen(entry.QueryID) {
			profilesSkippedTotal.WithLabelValues("dedup").Inc()
			if entry.EventTime > maxProcessedEventTime {
				maxProcessedEventTime = entry.EventTime
			}
			continue
		}

		// Pre-mark as seen before launching goroutine to prevent duplicate
		// fetches within the same poll cycle.
		c.markSeen(entry.QueryID)

		// Acquire semaphore slot.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break
		}

		fetchWg.Add(1)
		go func(e queryDetailEntry) {
			defer fetchWg.Done()
			defer func() { <-sem }()

			profileText, err := c.fetchProfile(ctx, feHost, e.QueryID)
			if err != nil {
				if errors.Is(err, errProfileNotFound) {
					slog.Debug("profile not found (not collected or evicted)", "query_id", e.QueryID, "fe_host", feHost)
					profilesSkippedTotal.WithLabelValues("not_found").Inc()
				} else {
					slog.Warn("failed to fetch profile", "query_id", e.QueryID, "fe_host", feHost, "error", err)
					profilesSkippedTotal.WithLabelValues("fetch_error").Inc()
				}
				return
			}
			resultsCh <- fetchResult{entry: e, profileText: profileText}
		}(entry)
	}

	// Wait for all fetches to complete, then close the results channel.
	fetchWg.Wait()
	close(resultsCh)

	// Process results and emit to writer.
	for r := range resultsCh {
		var queryHash string
		if r.entry.SQL != "" {
			queryHash = fmt.Sprintf("%x", md5.Sum([]byte(r.entry.SQL)))
		}

		startTimeStr := ""
		if r.entry.StartTime > 0 {
			startTimeStr = time.UnixMilli(r.entry.StartTime).UTC().Format(time.RFC3339)
		}
		endTimeStr := ""
		if r.entry.EndTime > 0 {
			endTimeStr = time.UnixMilli(r.entry.EndTime).UTC().Format(time.RFC3339)
		}
		durationMs := float64(0)
		if r.entry.Latency > 0 {
			durationMs = float64(r.entry.Latency)
		}

		profileEntry := ProfileEntry{
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
			FEHost:      feHost,
			Cluster:     c.cfg.ClusterName,
			SRQueryID:   r.entry.QueryID,
			State:       r.entry.State,
			Database:    r.entry.Database,
			User:        r.entry.User,
			StartTime:   startTimeStr,
			EndTime:     endTimeStr,
			DurationMs:  durationMs,
			SQLText:     r.entry.SQL,
			QueryHash:   queryHash,
			ProfileText: r.profileText,
		}

		c.writer.Write(profileEntry)
		profilesCollectedTotal.WithLabelValues(feHost).Inc()

		if r.entry.EventTime > maxProcessedEventTime {
			maxProcessedEventTime = r.entry.EventTime
		}
	}

	if maxProcessedEventTime > 0 {
		c.lastEventTimeMu.Lock()
		c.lastEventTime[feHost] = maxProcessedEventTime
		c.lastEventTimeMu.Unlock()
	}

	pollDurationSeconds.WithLabelValues(feHost).Observe(time.Since(start).Seconds())
}

// doHTTPWithRetry executes an HTTP request with up to cfg.HTTPRetries retries
// on transient errors (connection errors, 5xx status). Returns the response on
// success. Caller must close the response body.
func (c *Collector) doHTTPWithRetry(ctx context.Context, req *http.Request) (*http.Response, error) {
	maxAttempts := 1 + c.cfg.HTTPRetries
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Short exponential backoff: 200ms, 400ms, ...
			backoff := time.Duration(200<<uint(attempt-1)) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Clone the request for retry (body is nil for GETs so this is safe).
		retryReq := req.Clone(ctx)
		resp, err := c.httpClient.Do(retryReq)
		if err != nil {
			lastErr = err
			continue
		}

		// Retry on 5xx server errors.
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

// fetchQueryDetails calls GET /api/query_detail on the given FE host.
// eventTime is a nanosecond timestamp; entries with eventTime > this value are returned.
// Pass 0 to get all cached entries.
func (c *Collector) fetchQueryDetails(ctx context.Context, feHost string, eventTime int64) ([]queryDetailEntry, error) {
	url := fmt.Sprintf("http://%s/api/query_detail?event_time=%d", feHost, eventTime)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", c.authHeader)

	resp, err := c.doHTTPWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxQueryDetailResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	// Parse defensively — try array first, fall back to empty.
	var entries []queryDetailEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		// Some SR versions wrap the array in an object with a "data" key.
		var wrapper struct {
			Data []queryDetailEntry `json:"data"`
		}
		if err2 := json.Unmarshal(body, &wrapper); err2 != nil {
			return nil, fmt.Errorf("parsing response (tried array and {data:[]}): array=%v, wrapper=%v", err, err2)
		}
		entries = wrapper.Data
	}

	return entries, nil
}

// fetchProfile calls GET /api/profile?query_id=<id> and returns the plain text profile.
func (c *Collector) fetchProfile(ctx context.Context, feHost string, queryID string) (string, error) {
	url := fmt.Sprintf("http://%s/api/profile?query_id=%s", feHost, neturl.QueryEscape(queryID))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", c.authHeader)

	resp, err := c.doHTTPWithRetry(ctx, req)
	if err != nil {
		return "", fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", errProfileNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	// Profile endpoint returns plain text.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProfileResponseBytes))
	if err != nil {
		return "", fmt.Errorf("reading profile: %w", err)
	}

	return string(body), nil
}

// seen returns true if the query ID has already been processed.
func (c *Collector) seen(queryID string) bool {
	c.dedupMu.RLock()
	defer c.dedupMu.RUnlock()
	_, ok := c.dedup[queryID]
	return ok
}

// markSeen records a query ID as processed. If the dedup map exceeds
// MaxDedupEntries, the oldest 10% of entries are evicted immediately.
func (c *Collector) markSeen(queryID string) {
	c.dedupMu.Lock()
	defer c.dedupMu.Unlock()

	c.dedup[queryID] = time.Now()

	maxEntries := c.cfg.MaxDedupEntries
	if maxEntries > 0 && len(c.dedup) > maxEntries {
		c.emergencyEvictLocked(maxEntries)
	}
}

// emergencyEvictLocked removes the oldest 10% of dedup entries when the map
// exceeds its size cap. Must be called with dedupMu held.
func (c *Collector) emergencyEvictLocked(maxEntries int) {
	toEvict := maxEntries / 10
	if toEvict < 1 {
		toEvict = 1
	}

	type entry struct {
		id string
		ts time.Time
	}
	entries := make([]entry, 0, len(c.dedup))
	for id, ts := range c.dedup {
		entries = append(entries, entry{id, ts})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ts.Before(entries[j].ts)
	})

	for i := 0; i < toEvict && i < len(entries); i++ {
		delete(c.dedup, entries[i].id)
	}

	slog.Warn("emergency dedup eviction", "evicted", toEvict, "cap", maxEntries, "remaining", len(c.dedup))
}

// evictExpiredDedup removes query IDs older than DedupTTL from the dedup map.
func (c *Collector) evictExpiredDedup() {
	c.dedupMu.Lock()
	defer c.dedupMu.Unlock()

	cutoff := time.Now().Add(-c.cfg.DedupTTL)
	evicted := 0
	for id, ts := range c.dedup {
		if ts.Before(cutoff) {
			delete(c.dedup, id)
			evicted++
		}
	}
	if evicted > 0 {
		slog.Info("dedup TTL eviction", "evicted", evicted, "remaining", len(c.dedup))
	}
}

// isBackedOff returns true if the FE host is currently in a backoff window.
func (c *Collector) isBackedOff(feHost string) bool {
	c.backoffMu.Lock()
	defer c.backoffMu.Unlock()
	b, ok := c.backoff[feHost]
	if !ok {
		return false
	}
	return time.Now().Before(b.nextRetryAt)
}

// nextRetryAt returns the time at which the FE host can be retried.
func (c *Collector) nextRetryAt(feHost string) time.Time {
	c.backoffMu.Lock()
	defer c.backoffMu.Unlock()
	if b, ok := c.backoff[feHost]; ok {
		return b.nextRetryAt
	}
	return time.Time{}
}

// recordFailure increments the failure count for an FE host and extends the
// backoff window using exponential backoff capped at backoffMax.
func (c *Collector) recordFailure(feHost string) {
	c.backoffMu.Lock()
	defer c.backoffMu.Unlock()

	b, ok := c.backoff[feHost]
	if !ok {
		b = &feBackoff{currentDelay: backoffInitial}
		c.backoff[feHost] = b
	}

	b.failures++
	b.nextRetryAt = time.Now().Add(b.currentDelay)
	slog.Warn("FE backoff", "fe_host", feHost, "consecutive_failures", b.failures,
		"next_retry_in", b.currentDelay.String())

	// Increase delay for next failure, capped at backoffMax.
	b.currentDelay = time.Duration(float64(b.currentDelay) * backoffFactor)
	if b.currentDelay > backoffMax {
		b.currentDelay = backoffMax
	}
}

// resetBackoff clears the backoff state for an FE host after a successful poll.
func (c *Collector) resetBackoff(feHost string) {
	c.backoffMu.Lock()
	defer c.backoffMu.Unlock()
	if _, ok := c.backoff[feHost]; ok {
		delete(c.backoff, feHost)
	}
}

// ParseQueryDetails parses a JSON byte slice of query detail entries.
// Exported for testing.
func ParseQueryDetails(data []byte) ([]queryDetailEntry, error) {
	var entries []queryDetailEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		var wrapper struct {
			Data []queryDetailEntry `json:"data"`
		}
		if err2 := json.Unmarshal(data, &wrapper); err2 != nil {
			return nil, fmt.Errorf("parsing response: array=%v, wrapper=%v", err, err2)
		}
		entries = wrapper.Data
	}
	return entries, nil
}
