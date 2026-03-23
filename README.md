# StarRocks Profile Collector

[![CI](https://github.com/trmlabs/starrocks-profile-collector/actions/workflows/ci.yml/badge.svg)](https://github.com/trmlabs/starrocks-profile-collector/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/trmlabs/starrocks-profile-collector)](https://goreportcard.com/report/github.com/trmlabs/starrocks-profile-collector)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A standalone service that continuously collects query execution profiles from [StarRocks](https://www.starrocks.io/) FE nodes and writes them to object storage or local files as Hive-partitioned JSONL. Designed for long-term query performance analysis and observability.

## Why?

StarRocks collects detailed query execution profiles, but only retains them in FE memory briefly. This tool captures profiles before they are evicted, enabling:

- **Query performance trending** over days, weeks, or months
- **Slow query analysis** with full execution plan details
- **Cross-system correlation** via SQL hash matching
- **BigQuery/Trino/DuckDB analytics** over Hive-partitioned JSONL files

## Architecture

```
                    StarRocks Cluster
                    ┌──────────────────────┐
                    │  FE-0    FE-1   FE-2 │
                    │  :8030   :8030  :8030 │
                    └────┬───────┬──────┬───┘
                         │       │      │
              ┌──────────┴───────┴──────┴──────────┐
              │     StarRocks Profile Collector     │
              │                                     │
              │  Poll /api/query_detail (per FE)    │
              │  Fetch /api/profile?query_id=...    │
              │  Dedup across FEs + poll cycles     │
              │  Buffer → batch → flush             │
              │                                     │
              │  :9091/metrics  /health  /ready     │
              └──────────────┬──────────────────────┘
                             │
               ┌─────────────┼─────────────┐
               ▼             ▼             ▼
           GCS bucket    S3 bucket    Local files
           (JSONL)       (coming)      (JSONL)
```

## Quick Start

### Docker Compose (local StarRocks + collector)

```bash
docker compose -f deploy/docker-compose.yml up
```

This starts a StarRocks all-in-one instance and the collector writing to `./deploy/output/`. Then:

```sql
-- Connect to StarRocks
mysql -h 127.0.0.1 -P 9030 -u root

-- Enable profiling
SET GLOBAL enable_profile = true;
ADMIN SET FRONTEND CONFIG("enable_collect_query_detail_info" = "true");

-- Run some queries, then check output
-- ls ./deploy/output/starrocks-query-profiles/
```

### Binary

```bash
export CLUSTER_NAME=my-cluster
export FE_HOSTS=fe-0:8030,fe-1:8030
export STORAGE_BACKEND=file
export OUTPUT_DIR=./output
make run
```

### Docker

```bash
docker run --rm \
  -e CLUSTER_NAME=my-cluster \
  -e FE_HOSTS=fe-0:8030 \
  -e STORAGE_BACKEND=stdout \
  ghcr.io/trmlabs/starrocks-profile-collector:latest
```

## How It Works

1. **Poll**: Every `POLL_INTERVAL`, the collector queries each FE node's `/api/query_detail` endpoint for recently executed queries.
2. **Dedup**: A query ID seen on one FE is not re-fetched from another FE (or on the next poll cycle). Dedup entries expire after `DEDUP_TTL`.
3. **Fetch Profile**: For each new query ID with state `FINISHED`, the collector fetches the full execution profile from `/api/profile?query_id=<id>`.
4. **Buffer & Flush**: Profile entries are buffered in memory and flushed as JSONL files on a timer or when the batch size threshold is reached.

## Storage Backends

Set `STORAGE_BACKEND` to choose where profiles are written:

| Backend  | Description                         | Required Config               |
|----------|-------------------------------------|-------------------------------|
| `gcs`    | Google Cloud Storage (default)      | `GCS_BUCKET`                  |
| `file`   | Local filesystem                    | `OUTPUT_DIR`                  |
| `stdout` | JSONL to stdout (for piping/debug)  | (none)                        |

All backends write Hive-style partitioned JSONL:
```
<prefix>/year=2025/month=01/day=15/hour=14/20250115_143022_a1b2c3d4.jsonl
```

## StarRocks Prerequisites

The collector relies on two FE HTTP APIs that must be enabled:

### `enable_collect_query_detail_info` (FE Config)

Enables the `/api/query_detail` HTTP endpoint. **Without this, the collector cannot discover any queries.**

```sql
ADMIN SET FRONTEND CONFIG("enable_collect_query_detail_info" = "true");
```

**Default**: `false`. To persist across FE restarts, add to `fe.conf`:
```properties
enable_collect_query_detail_info = true
```

### `enable_profile` (Session Variable)

Turns on query profile collection. Without this, `/api/profile` returns 404.

```sql
SET GLOBAL enable_profile = true;
```

### `big_query_profile_threshold` (Recommended)

Only profile queries exceeding this duration. Recommended for production to avoid overhead on trivial queries.

```sql
SET GLOBAL big_query_profile_threshold = '500ms';
```

## Configuration

All settings are read from environment variables:

| Variable | Description | Default |
|---|---|---|
| `CLUSTER_NAME` | Identifier for this StarRocks cluster (e.g. "my-cluster") | **(required)** |
| `FE_HOSTS` | Comma-separated FE HTTP endpoints (e.g. "fe-0:8030,fe-1:8030") | `""` |
| `FE_HEADLESS_SERVICE` | Headless K8s service for dynamic FE discovery. Takes precedence over `FE_HOSTS`. | `""` |
| `FE_HTTP_PORT` | FE HTTP port, used with `FE_HEADLESS_SERVICE` | `8030` |
| `FE_USER` | StarRocks user for HTTP API auth | `root` |
| `FE_PASSWORD` | StarRocks password | `""` |
| `POLL_INTERVAL` | How often to poll FEs (Go duration, e.g. "5s") | `5s` |
| `STORAGE_BACKEND` | Storage backend: `gcs`, `file`, or `stdout` | `gcs` |
| `GCS_BUCKET` | GCS bucket (required for `gcs` backend) | `""` |
| `GCS_PREFIX` | Path prefix within GCS bucket | `starrocks-query-profiles` |
| `OUTPUT_DIR` | Base directory for `file` backend | `./output` |
| `FILE_PREFIX` | Path prefix within `OUTPUT_DIR` | `starrocks-query-profiles` |
| `FLUSH_INTERVAL` | How often to flush to storage (Go duration) | `120s` |
| `BATCH_SIZE` | Max entries before forced flush | `1000` |
| `BUFFER_SIZE` | In-memory buffer size | `10000` |
| `DEDUP_TTL` | How long to remember seen query IDs (Go duration) | `30m` |
| `MAX_DEDUP_ENTRIES` | Max dedup map entries before oldest 10% are evicted | `100000` |
| `MAX_CONCURRENT_FETCHES` | Max concurrent profile fetches per poll cycle | `10` |
| `HTTP_RETRIES` | Number of retries for transient HTTP errors (5xx, connection) | `1` |
| `METRICS_PORT` | Prometheus metrics port | `:9091` |
| `CHECKPOINT_DIR` | Directory for persisting lastEventTime across restarts (empty = no persistence) | `""` |

## Output Format

Each JSONL line contains:

```json
{
  "ts": "2025-01-15T14:30:22Z",
  "fe_host": "fe-0:8030",
  "cluster": "my-cluster",
  "sr_query_id": "abc-123-def",
  "state": "FINISHED",
  "database": "my_db",
  "user": "analyst",
  "start_time": "2025-01-15T14:30:00Z",
  "end_time": "2025-01-15T14:30:22Z",
  "duration_ms": 22000,
  "sql_text": "SELECT * FROM ...",
  "query_hash": "a1b2c3d4e5f6...",
  "profile_text": "Query:\n  Summary:\n    ..."
}
```

The `query_hash` field is an MD5 hash of the SQL text, useful for correlating queries across systems (e.g., joining with SQL proxy logs). It is omitted when `sql_text` is empty.

## BigQuery External Table

Create an external table over GCS data for analysis:

```sql
CREATE OR REPLACE EXTERNAL TABLE `your_project.your_dataset.starrocks_query_profiles`
(
  ts STRING,
  fe_host STRING,
  cluster STRING,
  sr_query_id STRING,
  state STRING,
  `database` STRING,
  `user` STRING,
  start_time STRING,
  end_time STRING,
  duration_ms FLOAT64,
  sql_text STRING,
  query_hash STRING,
  profile_text STRING
)
WITH PARTITION COLUMNS (
  year STRING,
  month STRING,
  day STRING,
  hour STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://YOUR_BUCKET/starrocks-query-profiles/*'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/starrocks-query-profiles/'
);
```

## Prometheus Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `profile_collector_polls_total` | Counter | `fe_host` | Total poll cycles |
| `profile_collector_profiles_collected_total` | Counter | `fe_host` | Profiles collected |
| `profile_collector_profiles_skipped_total` | Counter | `reason` | Profiles skipped (dedup, fetch_error, not_found) |
| `profile_collector_poll_duration_seconds` | Histogram | `fe_host` | Poll cycle duration |
| `profile_collector_flushes_total` | Counter | `status` | Flush operations (success, error) |
| `profile_collector_active` | Gauge | -- | 1 when collector is running |
| `profile_collector_bytes_written_total` | Counter | -- | Total bytes written |
| `profile_collector_entries_per_flush` | Histogram | -- | Entries per flush |
| `profile_collector_flush_duration_seconds` | Histogram | -- | Flush duration |

Endpoints:
- `GET /metrics` -- Prometheus metrics
- `GET /health` -- Liveness probe
- `GET /ready` -- Readiness probe (OK after first successful FE poll)

## Deployment

### Kubernetes

See [`deploy/kubernetes.yaml`](deploy/kubernetes.yaml) for a complete example with Deployment, Service, probes, and an optional ServiceMonitor.

### Requirements

1. Network access to all FE nodes on their HTTP port (default 8030)
2. Storage access (GCS credentials via Workload Identity/ADC, or writable local directory)
3. `CLUSTER_NAME` and either `FE_HOSTS` or `FE_HEADLESS_SERVICE` set

## Build

```bash
make build          # Current platform
make build-linux    # Linux amd64
make test           # Run all tests
make docker-build   # Multi-stage Docker build
make help           # Show all targets
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
