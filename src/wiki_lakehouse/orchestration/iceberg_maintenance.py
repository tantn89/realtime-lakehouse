"""Iceberg table maintenance, submitted to Spark through Spark Connect.

A stream commits every few seconds, so each commit adds small files and a snapshot. Left
alone, reads slow down (thousands of tiny Parquet files) and metadata grows without bound.
- rewrite_data_files compacts small files into ~128 MB targets (new "replace" snapshots,
  which the downstream streams are configured to skip);
- expire_snapshots drops history older than an hour, keeping enough for the streams, which
  lag by seconds, to resume from their checkpointed snapshot.

Not done here: remove_orphan_files lists table directories through Hadoop's FileSystem, which
needs the S3A connector and the AWS SDK bundle (~300 MB) in the Spark image. Orphans only
come from failed commits, so this is housekeeping, not correctness; it is on the roadmap.
"""

# No `from __future__ import annotations`: Dagster inspects the real type of `context`.
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from dagster import MetadataValue, OpExecutionContext, job, op

TABLES = ["bronze.recentchange_raw", "silver.edits", "silver.editor_sessions",
          "ops.stream_progress"]


def _spark() -> Any:
    from pyspark.sql import SparkSession  # Spark Connect client: no JVM in this image

    return SparkSession.builder.remote(
        os.environ.get("LAKEHOUSE_SPARK_CONNECT", "sc://spark-connect:15002")).getOrCreate()


def _file_stats(spark: Any, table: str) -> tuple[int, float]:
    row = spark.sql(f"SELECT count(*) AS files, coalesce(avg(file_size_in_bytes), 0) AS avg_bytes "
                    f"FROM lake.{table}.files").collect()[0]
    return int(row["files"]), float(row["avg_bytes"])


@op
def maintain_tables(context: OpExecutionContext) -> None:
    spark = _spark()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    summary: dict[str, Any] = {}
    for table in TABLES:
        files_before, avg_before = _file_stats(spark, table)
        rewrite = spark.sql(f"""
            CALL lake.system.rewrite_data_files(table => '{table}', options => map(
                'min-input-files', '5', 'partial-progress.enabled', 'true',
                'target-file-size-bytes', '134217728'))""").collect()[0]
        expire = spark.sql(f"""
            CALL lake.system.expire_snapshots(table => '{table}',
                older_than => TIMESTAMP '{now - timedelta(hours=1):%Y-%m-%d %H:%M:%S}',
                retain_last => 20)""").collect()[0]
        files_after, avg_after = _file_stats(spark, table)
        summary[table] = {
            "files_before": files_before, "files_after": files_after,
            "avg_kb_before": round(avg_before / 1024, 1),
            "avg_kb_after": round(avg_after / 1024, 1),
            "rewritten_files": rewrite["rewritten_data_files_count"],
            "expired_data_files": expire["deleted_data_files_count"],
        }
        context.log.info(f"{table}: {summary[table]}")
    context.add_output_metadata({"tables": MetadataValue.json(summary)})


@job(description="Compact small files and expire old snapshots on every table.")
def iceberg_maintenance() -> None:
    maintain_tables()
