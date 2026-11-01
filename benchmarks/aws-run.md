# AWS run, 2026-11-01

The same compose stack on one EC2 instance (`t3.xlarge`, ap-southeast-2), with S3 for storage
and the Glue Data Catalog for Iceberg (`docker-compose.aws.yml`, `terraform/`). The input was the live
Wikimedia recentchange stream. The run lasted 2 h 10 min, from 01:27 to 03:37 UTC, and the
infrastructure was destroyed afterwards.

This run predates the silver filter to `edit` and `new` events, so silver held every
recentchange type (categorize and log too). The counts and rates below are recentchange
events. Freshness, duplicates and orchestration results do not depend on that filter.

## Pipeline

| Metric | Value |
|---|---|
| Recentchange events ingested (bronze = silver) | 868,073 |
| Duplicate Kafka offsets in bronze / duplicate events in silver | 0 / 0 |
| Average / peak rate | 118 edits/s / 7,893 edits in one minute |
| Bot share | 0.90, mostly one bot run on enwiktionary |
| Freshness, `processed_at - event_time` (Athena, all rows) | p50 15.4 s, p95 20.0 s, p99 20.4 s |
| Micro-batch duration p95 (bronze / silver / sessions) | 1.2 s / 2.0 s / 2.7 s |
| Late rows dropped by the watermark | 0 |
| Editor sessions closed | 4,827 |

## Orchestration

All Dagster runs in this window succeeded:

| Job | Runs |
|---|---|
| `refresh_gold` (dbt on DuckDB, reading Glue through its Iceberg REST endpoint) | 19 |
| `lakehouse_checks` (4 asset checks) | 20 |
| `iceberg_maintenance` (compaction and snapshot expiry through Spark Connect) | 3 |

The first manual compaction took bronze from 157 files to 6 and silver from 162 files to 7. Two
hours of 10-second triggers left about 520 files per table between maintenance runs.

## Same tables, three engines

- **Spark**: GlueCatalog, with writes from the streams and reads from maintenance and status
  checks.
- **DuckDB**: `ATTACH '<account>' (TYPE iceberg, ENDPOINT_TYPE 'glue')`, with credentials from
  the instance profile. dbt passed 16/16.
- **Athena**: queries over all of silver took 1–2 s and scanned 0.6–6 MB each.

## Cost

| Item | Estimate |
|---|---|
| EC2 t3.xlarge, about 2.3 h at $0.2112/h | $0.49 |
| EBS 40 GB gp3 and public IPv4 for the same hours | $0.02 |
| S3, Glue, Athena (under 30 MB scanned), SSM | < $0.01 |
| **Total** | **≈ $0.52** |

## Issues found on the first boot

- **Dagster on a fresh volume.** The webserver and the daemon migrated the SQLite storage at the
  same time and left two alembic heads, so the daemon could not load the code. The fix makes the
  daemon wait for a healthy webserver.
- **Commit-time freshness after compaction.** The metric maps each row to the snapshot that added
  its current file. A row in a rewritten file maps to the compaction snapshot, so the metric
  reports minutes instead of seconds. `benchmarks/results.md` measures it before compaction; on a
  compacted table, use `processed_at`.
