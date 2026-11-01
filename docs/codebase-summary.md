# Codebase summary

A map of where things live. Open the file for behavior; each module has a docstring that states
its purpose and invariants.

## Python package: `src/wiki_lakehouse/`

| Path | Role |
|---|---|
| `producer/sse_stream.py` | SSE client for `stream.wikimedia.org`: parsing, reconnect, backoff, `Last-Event-ID`. |
| `producer/event_routing.py` | Routes a raw event to the main topic, the DLQ, or drops canaries. Loads the JSON schema. |
| `producer/kafka_publisher.py` | `Publisher` protocol, idempotent `KafkaPublisher`, in-memory `ListPublisher`, `DeliveryError`. |
| `producer/resume_cursor.py` | Resume position saved only after a confirmed flush. |
| `producer/producer_runs.py` | `publish_events` (live), `publish_summary` and `replay_bodies` (replay, benchmarks). |
| `producer/__main__.py` | CLI: `live` and `replay` subcommands. |
| `contracts/recentchange.schema.json` | The data contract, shipped inside the package. |
| `streaming/lakehouse_tables.py` | Table DDL and namespace creation. |
| `streaming/edit_transforms.py` | Pure DataFrame transforms: bronze, silver, dedup, sessions. Constants `WATERMARK`, `SESSION_GAP`, `EDIT_TYPES`. |
| `streaming/streaming_queries.py` | The three queries and the `ops.stream_progress` recorder. |
| `streaming/__main__.py` | Runs the three queries; handles SIGTERM; writes progress every 30 s. |
| `orchestration/definitions.py` | Dagster assets, jobs, schedules, resources. |
| `orchestration/lakehouse_checks.py` | The four asset checks. |
| `orchestration/lakehouse_duckdb.py` | DuckDB connection with the Iceberg catalog attached (REST or Glue). |
| `orchestration/iceberg_maintenance.py` | Compaction and guarded snapshot expiry job. |
| `reliability/scenario_publisher.py` | Publishes traffic with a known expected outcome (base, duplicates, poison, late). |
| `reliability/verify_exactly_once.py` | Verifies counts after a scenario; exits 1 on failure. |
| `benchmarks/freshness_report.py` | Commit-time freshness per phase, and silver compaction. |
| `benchmarks/duckdb_query_latency.py` | Median DuckDB query latency on silver. |
| `spark_session.py` | `LakehouseSettings` from `LAKEHOUSE_*` env vars; Iceberg catalog conf. |
| `spark_connect_server.py` | Starts a Spark Connect server for the orchestrator. |
| `smoke_check.py`, `lakehouse_status.py` | Operator commands (see [deployment-guide.md](deployment-guide.md)). |

## dbt project: `dbt/`

| Path | Role |
|---|---|
| `dbt_project.yml`, `profiles.yml` | Staging as views, marts as tables in schema `gold`; DuckDB file from `LAKEHOUSE_GOLD_DB`. |
| `macros/attach_lakehouse.sql` | Attaches the Iceberg catalog as `lake` before every run. |
| `macros/generate_schema_name.sql` | Uses the configured schema as is (`gold`, `staging`). |
| `models/staging/` | `sources.yml` (silver tables) and `stg_edits.sql`. |
| `models/marts/` | Four marts and `marts.yml` (descriptions and column tests). |
| `tests/` | Three singular tests: one row per minute and wiki, bot share in [0, 1], `edits_per_minute` reconciles with silver. |

## Scripts, tests, data

| Path | Role |
|---|---|
| `scripts/prove_exactly_once.py` | Crash-and-verify scenario in an isolated compose project. Standard library only. |
| `scripts/run_benchmarks.py` | Rate ramp, freshness and compaction; writes `benchmarks/results.json` and `results.md`. |
| `tests/test_producer.py` | Producer unit tests; no broker or Spark needed. |
| `tests/test_edit_transforms.py` | Spark transform tests; skipped when PySpark is absent. |
| `data/recordings/sample.jsonl` | 400 recorded events used by replay, tests and the proof. |
| `benchmarks/` | Committed results: [results.md](../benchmarks/results.md), `results.json`, [aws-run.md](../benchmarks/aws-run.md). |

## Infrastructure

| Path | Role |
|---|---|
| `docker-compose.yml` | The full local stack. |
| `docker-compose.it.yml` | Overlay for test and benchmark stacks: no published ports, small batches, no restart. |
| `docker-compose.aws.yml` | Overlay for AWS: Glue catalog, instance-profile credentials. |
| `docker/spark/`, `docker/orchestration/`, `docker/producer/` | Images: Spark with Iceberg and Kafka jars; Dagster, dbt and DuckDB; slim producer. |
| `docker/seaweedfs/s3.json` | Local-only S3 identity for the throwaway store. |
| `terraform/` | S3 bucket, Glue databases, Athena workgroup, IAM, EC2 with a bootstrap script, budget. |

## CI: `.github/workflows/ci.yml`

| Job | What it checks |
|---|---|
| `lint-and-unit` | `ruff check src tests scripts`, `mypy`, `pytest` on Python 3.11. |
| `terraform` | `fmt -check`, `init -backend=false`, `validate`. No credentials needed. |
| `orchestration` | All compose overlays render; the orchestration image builds (it runs `dbt parse`); Dagster definitions load. |
| `spark-and-exactly-once` | Builds images, runs the Spark unit tests inside the Spark image, then runs the crash proof. |
