"""Iceberg table maintenance, submitted to Spark through Spark Connect.

A stream commits every few seconds, so each commit adds small files and a snapshot. Left
alone, reads slow down (thousands of tiny Parquet files) and metadata grows without bound.
- rewrite_data_files compacts small files into ~128 MB targets (new "replace" snapshots,
  which the downstream streams are configured to skip);
- expire_snapshots drops history older than SNAPSHOT_RETENTION (3 days).

Two tables are also streaming sources: silver reads bronze, sessions reads silver.edits.
Their checkpoints store an Iceberg snapshot id, so expiring that snapshot would leave the
consumer unable to resume. A healthy consumer is seconds behind, far inside the retention,
but a stopped or catching-up one can be arbitrarily far behind. So expiry is skipped for a
source table whose consumer has not committed a batch (ops.stream_progress) for
CONSUMER_STALE_AFTER; it resumes once the consumer is healthy again. The guard sees
liveness, not position: a consumer that restarts after more than SNAPSHOT_RETENTION of
downtime looks fresh while it still has to read snapshots past the retention, so the
supported consumer downtime is under 3 days (the Kafka topic retains 7).

One table failing does not stop the others: errors are collected and the run fails at the
end with all of them.

Not done here: remove_orphan_files lists table directories through Hadoop's FileSystem, which
needs the S3A connector and the AWS SDK bundle (~300 MB) in the Spark image. Orphans only
come from failed commits, so this is housekeeping, not correctness; it is on the roadmap.
"""

# No `from __future__ import annotations`: Dagster inspects the real type of `context`.
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from dagster import Failure, MetadataValue, OpExecutionContext, job, op

TABLES = ["bronze.recentchange_raw", "silver.edits", "silver.editor_sessions",
          "ops.stream_progress"]
# Streaming source table -> the query (ops.stream_progress.query_name) that consumes it.
STREAM_CONSUMERS = {"bronze.recentchange_raw": "silver", "silver.edits": "sessions"}
SNAPSHOT_RETENTION = timedelta(days=3)
CONSUMER_STALE_AFTER = timedelta(minutes=15)


def _spark() -> Any:
    from pyspark.sql import SparkSession  # Spark Connect client: no JVM in this image

    return SparkSession.builder.remote(
        os.environ.get("LAKEHOUSE_SPARK_CONNECT", "sc://spark-connect:15002")).getOrCreate()


def _file_stats(spark: Any, table: str) -> tuple[int, float]:
    row = spark.sql(f"SELECT count(*) AS files, coalesce(avg(file_size_in_bytes), 0) AS avg_bytes "
                    f"FROM lake.{table}.files").collect()[0]
    return int(row["files"]), float(row["avg_bytes"])


def _consumer_lag_seconds(spark: Any) -> dict[str, int]:
    """Seconds since each consumer's newest batch. A consumer with no batch in the retention
    window is absent from the result, which callers treat as stale."""
    names = ", ".join(f"'{name}'" for name in STREAM_CONSUMERS.values())
    rows = spark.sql(f"""
        SELECT query_name,
               unix_timestamp(current_timestamp()) - unix_timestamp(max(batch_time)) AS lag_s
        FROM lake.ops.stream_progress
        WHERE query_name IN ({names})
          AND batch_time > current_timestamp() - INTERVAL {SNAPSHOT_RETENTION.days} DAYS
        GROUP BY query_name""").collect()
    return {row["query_name"]: int(row["lag_s"]) for row in rows}


def stale_consumer(table: str, lag_seconds: dict[str, int] | None) -> str | None:
    """Why expiry must be skipped for `table`, or None when it is safe to expire.
    `lag_seconds` is None when consumer progress could not be read."""
    consumer = STREAM_CONSUMERS.get(table)
    if consumer is None:
        return None
    if lag_seconds is None:
        return f"progress of consumer '{consumer}' could not be read"
    lag = lag_seconds.get(consumer)
    if lag is None:
        return f"consumer '{consumer}' has no batch in the last {SNAPSHOT_RETENTION.days} days"
    if lag > CONSUMER_STALE_AFTER.total_seconds():
        return f"consumer '{consumer}' last committed {lag}s ago"
    return None


def _maintain(spark: Any, table: str, expire_before: datetime,
              skip_expiry: str | None) -> dict[str, Any]:
    files_before, avg_before = _file_stats(spark, table)
    rewrite = spark.sql(f"""
        CALL lake.system.rewrite_data_files(table => '{table}', options => map(
            'min-input-files', '5', 'partial-progress.enabled', 'true',
            'target-file-size-bytes', '134217728'))""").collect()[0]
    result: dict[str, Any] = {"rewritten_files": rewrite["rewritten_data_files_count"]}
    if skip_expiry:
        result["expiry_skipped"] = skip_expiry
    else:
        expire = spark.sql(f"""
            CALL lake.system.expire_snapshots(table => '{table}',
                older_than => TIMESTAMP '{expire_before:%Y-%m-%d %H:%M:%S}',
                retain_last => 20)""").collect()[0]
        result["expired_data_files"] = expire["deleted_data_files_count"]
    files_after, avg_after = _file_stats(spark, table)
    return {"files_before": files_before, "files_after": files_after,
            "avg_kb_before": round(avg_before / 1024, 1),
            "avg_kb_after": round(avg_after / 1024, 1), **result}


@op
def maintain_tables(context: OpExecutionContext) -> None:
    spark = _spark()
    # Spark sessions run in UTC (spark.sql.session.timeZone), so a naive UTC literal is exact.
    expire_before = datetime.now(timezone.utc).replace(tzinfo=None) - SNAPSHOT_RETENTION
    lag_seconds: dict[str, int] | None
    try:
        lag_seconds = _consumer_lag_seconds(spark)
    except Exception as exc:  # unknown progress means "do not expire sources"
        context.log.warning(f"Could not read consumer progress, treating consumers as stale: {exc}")
        lag_seconds = None

    summary: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for table in TABLES:
        skip_expiry = stale_consumer(table, lag_seconds)
        if skip_expiry:
            context.log.warning(f"{table}: skipping snapshot expiry, {skip_expiry}")
        try:
            summary[table] = _maintain(spark, table, expire_before, skip_expiry)
        except Exception as exc:  # keep maintaining the other tables
            context.log.exception(f"{table}: maintenance failed")
            errors[table] = f"{type(exc).__name__}: {exc}"
            continue
        context.log.info(f"{table}: {summary[table]}")

    metadata = {"tables": MetadataValue.json(summary)}
    if errors:
        raise Failure(description=f"Maintenance failed for {sorted(errors)}",
                      metadata={**metadata, "errors": MetadataValue.json(errors)})
    context.add_output_metadata(metadata)


@job(description="Compact small files and expire snapshots older than 3 days on every table, "
                 "skipping expiry on a streaming source whose consumer is stale.")
def iceberg_maintenance() -> None:
    maintain_tables()
