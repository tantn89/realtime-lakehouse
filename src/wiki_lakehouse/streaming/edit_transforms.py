"""Pure DataFrame transformations, shared by the streaming jobs and their unit tests."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Only the fields the lakehouse uses; from_json ignores the rest, so upstream additions
# never break parsing. The producer's JSON-schema contract guarantees the required ones.
_BEFORE_AFTER = StructType([StructField("old", LongType()), StructField("new", LongType())])
RECENTCHANGE_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("id", StringType()), StructField("dt", StringType()),
        StructField("domain", StringType()),
    ])),
    StructField("id", LongType()), StructField("type", StringType()),
    StructField("namespace", IntegerType()), StructField("title", StringType()),
    StructField("timestamp", LongType()), StructField("user", StringType()),
    StructField("bot", BooleanType()), StructField("wiki", StringType()),
    StructField("server_name", StringType()), StructField("minor", BooleanType()),
    StructField("patrolled", BooleanType()), StructField("comment", StringType()),
    StructField("length", _BEFORE_AFTER), StructField("revision", _BEFORE_AFTER),
])

SESSION_GAP = "20 minutes"  # an editor idle this long has ended a session
WATERMARK = "10 minutes"  # events later than this behind the newest are dropped and counted


def kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
    )


def _event_time(dt: Column) -> Column:
    # meta.dt is ISO-8601 with milliseconds and a Z suffix, e.g. 2026-10-31T15:03:16.404Z.
    return F.to_timestamp(dt, "yyyy-MM-dd'T'HH:mm:ss[.SSS]X")


def bronze_to_edits(bronze_df: DataFrame) -> DataFrame:
    """Typed edit events. Deduplication is applied by the caller (it needs a watermark)."""
    e = F.from_json(F.col("value"), RECENTCHANGE_SCHEMA).alias("e")
    parsed = bronze_df.select(e, "kafka_partition", "kafka_offset", "ingested_at")
    return parsed.select(
        F.col("e.meta.id").alias("event_id"),
        _event_time(F.col("e.meta.dt")).alias("event_time"),
        F.col("e.id").alias("rc_id"),
        F.col("e.type").alias("event_type"),
        F.col("e.wiki").alias("wiki"),
        F.col("e.server_name").alias("server_name"),
        F.col("e.namespace").alias("namespace"),
        F.col("e.title").alias("title"),
        F.col("e.user").alias("user"),
        F.col("e.bot").alias("is_bot"),
        F.col("e.minor").alias("is_minor"),
        F.col("e.patrolled").alias("is_patrolled"),
        F.col("e.length.old").alias("length_old"),
        F.col("e.length.new").alias("length_new"),
        (F.col("e.length.new") - F.coalesce(F.col("e.length.old"), F.lit(0)))
        .alias("length_delta"),
        F.col("e.revision.old").alias("revision_old"),
        F.col("e.revision.new").alias("revision_new"),
        F.col("e.comment").alias("comment"),
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        F.current_timestamp().alias("processed_at"),
    ).where(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())


def deduplicate_edits(edits_df: DataFrame) -> DataFrame:
    """Drop repeats of the same Wikimedia event (streaming only: it needs a watermark).

    The producer resumes with Last-Event-ID and may resend a few events; they carry the
    same meta.id, so keeping the first within the watermark makes silver exactly-once per
    event, while state stays bounded to the watermark window.
    """
    return edits_df.withWatermark("event_time", WATERMARK).dropDuplicatesWithinWatermark(
        ["event_id"])


def edits_to_sessions(edits_df: DataFrame) -> DataFrame:
    """One row per editor session on a wiki: a burst of edits with gaps under SESSION_GAP."""
    window = F.session_window("event_time", SESSION_GAP)
    sessions = edits_df.groupBy(window.alias("session"), "wiki", "user").agg(
        F.count(F.lit(1)).alias("edits"),
        F.sum(F.abs(F.coalesce("length_delta", F.lit(0)))).alias("bytes_changed"),
        F.approx_count_distinct("title").alias("pages"),
        F.max(F.col("is_bot").cast("int")).cast("boolean").alias("is_bot"),
    )
    return sessions.select(
        "wiki", "user",
        F.col("session.start").alias("session_start"),
        F.col("session.end").alias("session_end"),
        "edits", "bytes_changed", "pages", "is_bot",
        F.current_timestamp().alias("processed_at"),
    )
