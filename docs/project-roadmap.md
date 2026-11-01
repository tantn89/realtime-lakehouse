# Roadmap

Status of the project and the next steps. Items are intent, not commitments, and carry no dates.

## Done (package version 0.1.0)

| Area | Delivered | Evidence |
|---|---|---|
| Ingest | Live SSE producer with contract, DLQ, canary filter, confirmed-delivery resume cursor, replay mode | `src/wiki_lakehouse/producer/`, `tests/test_producer.py` |
| Streaming | Bronze, silver (dedup, 10-minute watermark) and sessions (20-minute gap) on Iceberg | `src/wiki_lakehouse/streaming/`, `tests/test_edit_transforms.py` |
| Gold | Four dbt marts on DuckDB with singular tests | `dbt/` |
| Orchestration | Dagster schedules, four asset checks, hourly maintenance through Spark Connect | `src/wiki_lakehouse/orchestration/` |
| Reliability | Crash-and-verify proof, run in CI | [`scripts/prove_exactly_once.py`](../scripts/prove_exactly_once.py) |
| Benchmarks | Rate ramp to 15,000 target ev/s; compaction effect | [`benchmarks/results.md`](../benchmarks/results.md) |
| AWS | Terraform stack with cost guard; 2 h 10 min live run, about $0.52 | [`benchmarks/aws-run.md`](../benchmarks/aws-run.md), `terraform/` |
| CI | Lint, strict typing, unit tests, Terraform validate, compose render, Dagster load, exactly-once | `.github/workflows/ci.yml` |

## Next

| Item | Why it is not done | Notes |
|---|---|---|
| Databricks variant | Out of scope for 0.1.0 (see [project-overview-pdr.md](project-overview-pdr.md)) | The same tables on a managed engine would test the portability of the Iceberg layout. |
| `remove_orphan_files` in maintenance | Needs Hadoop's S3A connector and the AWS SDK bundle (about 300 MB) in the Spark image | Housekeeping, not correctness: orphans come only from failed commits. Context in `orchestration/iceberg_maintenance.py`. |
| Committed unit tests for the orchestration modules | They are verified ad hoc on a live stack today | Candidates: `stale_consumer` (pure), `_messages_since`, schedule skip logic in `definitions.py`. |
| Producer kill in the CI proof | The CI scenario SIGKILLs the stream only | Adding a producer kill would exercise the resume cursor end to end. |
| Faster producer | The Python producer topped out at about 9,600 ev/s for a 15,000 target ([results.md](../benchmarks/results.md)); the live feed is about 40 ev/s | Only matters for load tests. The cause (Python producer versus broker) was not isolated. |
| `warehouse-mcp` repository | Separate project | Would expose the gold marts to agents through MCP. The gold DuckDB file is already described as serving "dashboards and agents" (`dbt/dbt_project.yml`). Not started in this repo. |

## Known gaps worth knowing

- Late events beyond the watermark are dropped from silver; there is no automated replay from
  bronze.
- Silver freshness numbers are for a single desktop machine and a 10-second trigger.
- Orphan files accumulate after failed commits until `remove_orphan_files` is added.
