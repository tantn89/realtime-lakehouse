"""Iceberg table definitions, created idempotently before the streams start.

Tables are declared up front (not inferred by the first write) so partitioning, types and
format version are decisions in code review, not accidents of the first micro-batch.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

BRONZE = "bronze.recentchange_raw"
SILVER_EDITS = "silver.edits"
SILVER_SESSIONS = "silver.editor_sessions"
STREAM_PROGRESS = "ops.stream_progress"

# Readers parallelise per Parquet row group, not per file. With the 128 MB default a compacted
# file is a single row group, so DuckDB scans it on one thread and compaction made queries
# slower. 8 MB row groups keep big files splittable.
_ROW_GROUP_BYTES = str(8 * 1024 * 1024)

_TABLE_PROPERTIES = f"""TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.row-group-size-bytes' = '{_ROW_GROUP_BYTES}',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '50'
)"""

DDL = {
    BRONZE: f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            kafka_key STRING, value STRING, kafka_topic STRING, kafka_partition INT,
            kafka_offset BIGINT, kafka_ts TIMESTAMP, ingested_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(kafka_ts)) {_TABLE_PROPERTIES}""",
    SILVER_EDITS: f"""
        CREATE TABLE IF NOT EXISTS {SILVER_EDITS} (
            event_id STRING, event_time TIMESTAMP, rc_id BIGINT, event_type STRING,
            wiki STRING, server_name STRING, namespace INT, title STRING, user STRING,
            is_bot BOOLEAN, is_minor BOOLEAN, is_patrolled BOOLEAN,
            length_old BIGINT, length_new BIGINT, length_delta BIGINT,
            revision_old BIGINT, revision_new BIGINT, comment STRING,
            kafka_partition INT, kafka_offset BIGINT, ingested_at TIMESTAMP,
            processed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(event_time)) {_TABLE_PROPERTIES}""",
    SILVER_SESSIONS: f"""
        CREATE TABLE IF NOT EXISTS {SILVER_SESSIONS} (
            wiki STRING, user STRING, session_start TIMESTAMP, session_end TIMESTAMP,
            edits BIGINT, bytes_changed BIGINT, pages BIGINT, is_bot BOOLEAN,
            processed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(session_start)) {_TABLE_PROPERTIES}""",
    STREAM_PROGRESS: f"""
        CREATE TABLE IF NOT EXISTS {STREAM_PROGRESS} (
            query_name STRING, batch_id BIGINT, batch_time TIMESTAMP, num_input_rows BIGINT,
            input_rows_per_second DOUBLE, processed_rows_per_second DOUBLE,
            batch_duration_ms BIGINT, rows_dropped_by_watermark BIGINT,
            state_rows BIGINT
        ) USING iceberg {_TABLE_PROPERTIES}""",
}


def ensure_namespace(spark: SparkSession, namespace: str) -> None:
    """CREATE NAMESPACE IF NOT EXISTS that tolerates a concurrent creator.

    The REST catalog checks existence and inserts in two steps, so two processes starting
    together (stream and smoke check) can both pass the check; the loser gets a server error
    from the duplicate insert. If the namespace exists afterwards, the goal is met.
    """
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    except Exception:
        if not spark.sql(f"SHOW NAMESPACES LIKE '{namespace}'").collect():
            raise


def create_tables(spark: SparkSession) -> None:
    for namespace in {name.split(".")[0] for name in DDL}:
        ensure_namespace(spark, namespace)
    for table, ddl in DDL.items():
        spark.sql(ddl)
        # CREATE IF NOT EXISTS leaves tables from earlier versions untouched; bring the
        # write layout of existing tables up to date as well.
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES "
                  f"('write.parquet.row-group-size-bytes' = '{_ROW_GROUP_BYTES}')")
