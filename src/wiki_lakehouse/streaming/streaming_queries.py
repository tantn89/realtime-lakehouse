"""The three streaming queries and the recorder that turns their progress into a table.

Exactly-once: each query has its own checkpoint, and the Iceberg sink commits a micro-batch
atomically and records the batch id, so a batch replayed after a crash is not committed twice.
Kafka delivery into the pipeline is at-least-once (the producer can resend after a resume),
which silver absorbs by deduplicating on the Wikimedia event id within the watermark.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from wiki_lakehouse.streaming.edit_transforms import (
    bronze_to_edits,
    deduplicate_edits,
    edits_to_sessions,
    kafka_to_bronze,
)
from wiki_lakehouse.streaming.lakehouse_tables import (
    BRONZE,
    SILVER_EDITS,
    SILVER_SESSIONS,
    STREAM_PROGRESS,
)

TOPIC = "wiki.recentchange"
CHECKPOINTS = os.environ.get("LAKEHOUSE_CHECKPOINTS", "/checkpoints")
TRIGGER = os.environ.get("LAKEHOUSE_TRIGGER", "10 seconds")
# Options for the Iceberg-to-Iceberg hops (bronze -> silver -> sessions).
# - Iceberg's streaming source always skips REPLACE snapshots (compaction), and snapshot
#   expiry commits no snapshot at all. The two skip options only matter for DELETE / OVERWRITE
#   commits, which nothing in this repo issues; without them such a commit would fail the
#   query. Consequence: a future MERGE/UPDATE/DELETE on bronze would never reach silver.
# - The source resumes from the checkpointed snapshot, so it can only replay history that
#   snapshot expiry has kept.
# - max-files bounds a catch-up micro-batch (new checkpoint, long downtime). Do not also set
#   streaming-max-rows-per-micro-batch: Iceberg 1.6.1 mixes the two limits up when both are set.
_ICEBERG_STREAM_READ = {
    "streaming-skip-overwrite-snapshots": "true",
    "streaming-skip-delete-snapshots": "true",
    "streaming-max-files-per-micro-batch": os.environ.get("LAKEHOUSE_MAX_FILES_PER_BATCH", "100"),
}


def start_bronze(spark: SparkSession, bootstrap: str) -> StreamingQuery:
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.environ.get("LAKEHOUSE_MAX_OFFSETS", "100000"))
        .load()
    )
    return (
        kafka_to_bronze(kafka).writeStream.format("iceberg").outputMode("append")
        .queryName("bronze").trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
        .toTable(f"lake.{BRONZE}")
    )


def start_silver(spark: SparkSession) -> StreamingQuery:
    bronze = spark.readStream.format("iceberg").options(**_ICEBERG_STREAM_READ).load(
        f"lake.{BRONZE}")
    edits = deduplicate_edits(bronze_to_edits(bronze))
    return (
        edits.writeStream.format("iceberg").outputMode("append")
        .queryName("silver").trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"{CHECKPOINTS}/silver")
        .toTable(f"lake.{SILVER_EDITS}")
    )


def start_sessions(spark: SparkSession) -> StreamingQuery:
    edits = spark.readStream.format("iceberg").options(**_ICEBERG_STREAM_READ).load(
        f"lake.{SILVER_EDITS}")
    return (
        edits_to_sessions(edits).writeStream.format("iceberg").outputMode("append")
        .queryName("sessions").trigger(processingTime="30 seconds")
        .option("checkpointLocation", f"{CHECKPOINTS}/sessions")
        .toTable(f"lake.{SILVER_SESSIONS}")
    )


def progress_rows(recent_progress: Iterable[dict[str, Any]],
                  after_batch: int) -> list[dict[str, Any]]:
    """Rows for ops.stream_progress from a query's recentProgress, batches > `after_batch`."""
    rows = []
    for p in recent_progress:
        if p["batchId"] <= after_batch:
            continue
        operators = p.get("stateOperators") or []
        rows.append({
            "query_name": p["name"],
            "batch_id": p["batchId"],
            "batch_time": datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00"))
            .astimezone(timezone.utc).replace(tzinfo=None),
            "num_input_rows": p["numInputRows"],
            "input_rows_per_second": float(p.get("inputRowsPerSecond") or 0.0),
            "processed_rows_per_second": float(p.get("processedRowsPerSecond") or 0.0),
            "batch_duration_ms": p["durationMs"].get("triggerExecution", 0),
            "rows_dropped_by_watermark": sum(o.get("numRowsDroppedByWatermark", 0)
                                             for o in operators),
            "state_rows": sum(o.get("numRowsTotal", 0) for o in operators),
        })
    return rows


def record_progress(spark: SparkSession, queries: list[StreamingQuery],
                    last_batch: dict[str, int]) -> int:
    """Append new progress rows; `last_batch` advances only after the write succeeded.

    A failed write raises and leaves `last_batch` unchanged, so the next call retries the same
    batches (as long as they are still within recentProgress, the last 100 per query).
    """
    rows: list[dict[str, Any]] = []
    newest: dict[str, int] = {}
    for q in queries:
        new = progress_rows(q.recentProgress, last_batch.get(q.name, -1))
        if new:
            newest[q.name] = max(r["batch_id"] for r in new)
            rows.extend(new)
    if rows:
        spark.createDataFrame(rows, spark.table(f"lake.{STREAM_PROGRESS}").schema) \
            .writeTo(f"lake.{STREAM_PROGRESS}").append()
    last_batch.update(newest)
    return len(rows)
