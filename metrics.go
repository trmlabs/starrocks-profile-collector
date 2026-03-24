// Copyright 2025 TRM Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// metrics.go — Prometheus metric definitions for the StarRocks profile collector.
// All metrics use the "profile_collector_" prefix.
package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	pollsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "profile_collector_polls_total",
			Help: "Total number of poll cycles executed",
		},
		[]string{"fe_host"},
	)

	profilesCollectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "profile_collector_profiles_collected_total",
			Help: "Total number of query profiles collected",
		},
		[]string{"fe_host"},
	)

	profilesSkippedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "profile_collector_profiles_skipped_total",
			Help: "Total number of profiles skipped",
		},
		[]string{"reason"},
	)

	pollDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "profile_collector_poll_duration_seconds",
			Help:    "Duration of poll cycles in seconds",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
		[]string{"fe_host"},
	)

	flushesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "profile_collector_flushes_total",
			Help: "Total number of flush operations",
		},
		[]string{"status"},
	)

	collectorActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "profile_collector_active",
			Help: "Whether the profile collector is actively running (1=running, 0=stopped)",
		},
	)

	bytesWrittenTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "profile_collector_bytes_written_total",
			Help: "Total bytes written to the storage backend",
		},
	)

	entriesPerFlush = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "profile_collector_entries_per_flush",
			Help:    "Number of entries written per flush",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
	)

	flushDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "profile_collector_flush_duration_seconds",
			Help:    "Duration of flush operations in seconds",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	)

	lastFlushTimestamp = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "profile_collector_last_flush_timestamp_seconds",
			Help: "Unix timestamp of the last successful flush",
		},
	)

	fallbackWritesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "profile_collector_fallback_writes_total",
			Help: "Total entries written to fallback log due to storage backend errors",
		},
	)
)
