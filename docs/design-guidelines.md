# Data design guidelines

Rules for changing the data model. (This is a data project; there is no UI.) Architecture and
delivery semantics: [system-architecture.md](system-architecture.md).

## Layer contracts

| Layer | Contract | Never |
|---|---|---|
| Kafka main topic | Only events that pass the JSON schema, keyed by wiki | Carry invalid events; they go to the DLQ topic |
| Bronze | Raw value as a string plus Kafka coordinates (topic, partition, offset, timestamp) and `ingested_at`. Append-only, every event type | Parse or filter; bronze is the replay source |
| Silver | Typed, deduplicated `edit` and `new` events (`silver.edits`); closed sessions (`silver.editor_sessions`) | Contain `categorize` or `log` events, which would inflate edit and session counts |
| Gold | Business marts in a DuckDB file, rebuilt from silver at any time | Be a source of truth; losing the file loses nothing |
| `ops` | Pipeline telemetry (`ops.stream_progress`) | Be read as business data |

## Naming

- Tables: `<layer>.<snake_case_name>`; gold marts are named for the question they answer
  (`trending_articles`, `edits_per_minute`).
- Silver columns are domain names, not payload paths: `event_id`, `event_time`, `is_bot`,
  `length_delta`. Booleans start with `is_`.
- dbt: `stg_` for staging views, marts without prefix, singular tests as
  `assert_<invariant>.sql`.
- Kafka topics: `wiki.recentchange` and `wiki.recentchange.dlq`.

## The data contract

`src/wiki_lakehouse/contracts/recentchange.schema.json` is enforced at the producer
(`producer/event_routing.py`).

- It lists only the fields the pipeline depends on; extra fields are allowed so upstream additions
  never break ingestion. The Spark schema (`RECENTCHANGE_SCHEMA`) likewise reads only what it uses.
- `meta.dt` must be UTC with a `Z` suffix and optional milliseconds. The schema pins the shape;
  the producer also rejects impossible dates (month 13), because `format: date-time` is not
  checked by `jsonschema` without extra dependencies.
- A violation is never dropped silently: it goes to the DLQ with an `error` header (truncated to
  1000 characters). The `contract_violations_are_rare` check alerts above 1% of the last hour.
- To add a field: add it to the schema (and `required` only if the pipeline cannot work without
  it), to `RECENTCHANGE_SCHEMA`, to the `silver.edits` DDL, and to `bronze_to_edits`. Existing
  tables get new columns through an explicit `ALTER TABLE`, since `CREATE TABLE IF NOT EXISTS`
  does not change them.

## Idempotency and deduplication

- Producer: idempotent Kafka producer; at-least-once overall because a resume can resend events.
- Bronze: one row per Kafka offset, guaranteed by checkpoints plus Iceberg batch-id commits.
- Silver: the key is Wikimedia's `meta.id` (`event_id`), not `rc_id` (nullable in the contract).
  Dedup keeps the first within the watermark.
- Gold: `edits_per_minute` is incremental with `delete+insert` on (`event_minute`, `wiki`) and
  recomputes the last 20 minutes (the 10-minute watermark plus commit lag), so reruns are safe.
  The anchor is capped at the current minute so one future-dated event cannot push the window
  ahead of real time.
- Every idempotency claim gets a check. Today: `silver_has_no_duplicates`, the dbt test that
  guards the mart's grain, and the reconciliation test against silver.

## Time handling

- Everything is UTC: Spark sessions (`spark.sql.session.timeZone`), DuckDB (`SET TimeZone`) and
  dbt (`TimeZone: UTC`).
- **Event time** is `meta.dt`, parsed with `try_to_timestamp` so a bad value yields NULL and the
  row is dropped rather than failing the query. Windows, watermarks, partitions and gold marts use
  it.
- **Processing time** is `ingested_at` (bronze write) and `processed_at` (silver micro-batch start,
  which understates when a reader could first see the row). Use these for latency and freshness,
  never for business grouping.
- Watermark: 10 minutes. Late events are dropped from silver (and so from sessions and gold),
  counted, and kept in bronze. Changing it affects dedup state size, session emit delay, the
  `edits_per_minute` recompute window and the proof script's late scenario together.
- Sessions: 20-minute gap; `session_end` is the last edit's time; `pages` is approximate.

## Add a new gold mart

1. Add `dbt/models/marts/<name>.sql` selecting from `{{ ref('stg_edits') }}` or a silver source in
   `models/staging/sources.yml`. Default materialization is a table in schema `gold`.
2. Describe it and add column tests in `dbt/models/marts/marts.yml`.
3. For a rule that spans tables or time, add a singular test in `dbt/tests/`. Use a bounded
   window so the test stays cheap as history grows (see the reconciliation test).
4. If it is incremental, make the recompute window cover the watermark and use a capped anchor.
5. Nothing to register in Dagster: `@dbt_assets` reads the dbt manifest, which the orchestration
   image builds with `dbt parse`.
6. Check locally: bring up the stack and run the `refresh_gold` job from the Dagster UI.

## Add a new asset check

1. Add an `@asset_check` in `orchestration/lakehouse_checks.py`, bound to a silver or bronze
   asset key, returning `AssetCheckResult` with useful `metadata`.
2. Filter on a partition column (`event_time`) as well as the column you care about, so DuckDB
   does not scan every partition.
3. Choose severity deliberately: `WARN` for a trend (volume), failure for a broken invariant.
4. Append it to `CHECKS`; `definitions.py` and the `lakehouse_checks` job pick it up.
5. Do not add `from __future__ import annotations` to that module (see
   [code-standards.md](code-standards.md)).

## Add a new streaming table

Declare it in `streaming/lakehouse_tables.py` (`DDL` plus a constant), with partitioning and the
shared table properties, then add a query in `streaming_queries.py`, an `AssetSpec` in
`definitions.py`, and the table to `TABLES` in `iceberg_maintenance.py`. If another stream reads it,
add it to `STREAM_CONSUMERS` so snapshot expiry respects that consumer.
