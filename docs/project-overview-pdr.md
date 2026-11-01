# Project overview and requirements

A streaming lakehouse over the live Wikimedia `recentchange` feed. It is a portfolio project: the
goal is to show a small, correct, reproducible pipeline, not to serve production traffic.

Architecture: [system-architecture.md](system-architecture.md). Run it: [deployment-guide.md](deployment-guide.md).

## Problem

Wikipedia edits arrive as a continuous event stream. Turning that into queryable, trustworthy
tables needs more than "write to a table": duplicates after a producer resume, malformed events,
late events, small-file buildup in the table format, and checks that say when the pipeline is stuck.

## Goals

| Goal | Where it lives |
|---|---|
| Exactly-once into the lakehouse tables, with a crash test that proves it | `scripts/prove_exactly_once.py` |
| A data contract at the edge, with a dead-letter topic instead of silent loss | `src/wiki_lakehouse/contracts/`, `producer/event_routing.py` |
| Medallion layers on Apache Iceberg, readable by more than one engine | `streaming/lakehouse_tables.py`, `dbt/` |
| Orchestration with schedules, asset checks and table maintenance | `src/wiki_lakehouse/orchestration/` |
| One-command local run; the same code on AWS via Terraform | `docker-compose.yml`, `docker-compose.aws.yml`, `terraform/` |
| Measured, reproducible numbers rather than claims | `benchmarks/`, `scripts/run_benchmarks.py` |

## Non-goals

| Not in scope | Why |
|---|---|
| A Databricks variant | Same pipeline on a managed engine adds a vendor, not new ideas. Listed in the [roadmap](project-roadmap.md). |
| Flink | Spark Structured Streaming covers the stateful operators used here (dedup, session windows). |
| Grafana or other dashboards | Gold marts are the product; observability is Dagster asset checks plus the `ops.stream_progress` table. |
| MSK or Kinesis | Redpanda is Kafka-compatible and runs on one instance; the producer and Spark code only need a Kafka bootstrap address. |
| Machine learning | The data is modelled for analytics, not for training. |
| Authentication or multi-tenant security | Every published port is bound to `127.0.0.1` and nothing authenticates; see [deployment-guide.md](deployment-guide.md). |

## Functional requirements

Evidence classes: **code** = source or config in the repo, **test** = automated test, **proof** =
end-to-end scenario script, **measured** = committed benchmark output.

| ID | Requirement | Acceptance criterion | Evidence |
|---|---|---|---|
| F1 | Ingest the live feed and survive disconnects | Reconnects with exponential backoff and `Last-Event-ID`; resume position saved only after Kafka confirmed delivery | code `producer/sse_stream.py`, `producer/resume_cursor.py`; test `tests/test_producer.py` |
| F2 | Validate events against a contract | Contract violations land in `wiki.recentchange.dlq` with the reason in an `error` header; canary events are dropped | code `producer/event_routing.py`; test `tests/test_producer.py`; proof (25 poison events end in the DLQ) |
| F3 | Land every Kafka record in bronze | Every Kafka offset appears in bronze exactly once, also after a SIGKILL of the stream | proof [`scripts/prove_exactly_once.py`](../scripts/prove_exactly_once.py) |
| F4 | Typed, deduplicated `edit` and `new` events in silver | One row per Wikimedia event id within the 10-minute watermark; `categorize` and `log` stay in bronze only | code `streaming/edit_transforms.py`; test `tests/test_edit_transforms.py`; proof |
| F5 | Editor sessions | A session closes after a 20-minute gap and is appended once the watermark passes it | code `edits_to_sessions`; test `tests/test_edit_transforms.py` |
| F6 | Gold marts | Four dbt marts rebuilt every 5 minutes from silver into a DuckDB file | code `dbt/models/marts/`, `orchestration/definitions.py`; measured: dbt passed 16/16 on AWS ([aws-run.md](../benchmarks/aws-run.md)) |
| F7 | Health checks | Freshness (300 s), duplicates, volume floor, and DLQ share (< 1%) are asset checks | code `orchestration/lakehouse_checks.py` |
| F8 | Table maintenance | Hourly compaction; snapshot expiry (3 days) skipped for a source table whose consumer is stale | code `orchestration/iceberg_maintenance.py` |
| F9 | Same tables from three engines | Spark writes; DuckDB and Athena read the same Iceberg tables on AWS | measured [aws-run.md](../benchmarks/aws-run.md) |

## Non-functional requirements

| ID | Requirement | Acceptance criterion | Evidence |
|---|---|---|---|
| N1 | Freshness | Event time to commit, measured before compaction | measured [results.md](../benchmarks/results.md): p50 15.4 s to 16.6 s, p99 up to 21.6 s across four rates (100 to 15,000 ev/s target) |
| N2 | Throughput | Silver receives every expected edit/new event at each tested rate | measured [results.md](../benchmarks/results.md): achieved publish rate topped out at 9,596 ev/s for a 15,000 target, so the ceiling is the Python producer |
| N3 | Correctness on AWS | No duplicates, no late rows in a 2 h 10 min live run | measured [aws-run.md](../benchmarks/aws-run.md): 868,073 events, 0 duplicate offsets, 0 duplicate events |
| N4 | Cost | A full AWS demo stays small | measured [aws-run.md](../benchmarks/aws-run.md): about $0.52 for the run; `max_hours` timer and a monthly budget in `terraform/` |
| N5 | Reproducibility | A fresh clone runs with one command; CI rebuilds images and re-runs the crash proof | `docker-compose.yml`, `.github/workflows/ci.yml` |
| N6 | Code quality | Lint, strict typing and unit tests pass in CI | `pyproject.toml`, `.github/workflows/ci.yml` |
| N7 | Portability | Works on amd64 and arm64 | `docker/spark/Dockerfile` resolves the JDK path per architecture |

## Known limits

- Freshness figures come from a Windows desktop (20 logical CPUs) with a 10 s trigger; they are
  not a general capacity claim. Two chained 10 s triggers (bronze, then silver) fit the observed
  15 to 21 s.
- Silver drops events more than 10 minutes behind the newest event time seen; bronze keeps them.
- A consumer down longer than the 3-day snapshot retention cannot resume from its checkpoint
  (details in [system-architecture.md](system-architecture.md#failure-modes-and-recovery)).
- The orchestration modules are verified against a live stack, not by committed unit tests yet.
