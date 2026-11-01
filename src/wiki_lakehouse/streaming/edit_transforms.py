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

# recentchange also carries 'categorize' (a page added to a category: one per category an
# edit touches), 'log' (moves, blocks, uploads, ...) and others. They are not edits and would
# inflate edit and session counts, so they stay in bronze only.
EDIT_TYPES = ("edit", "new")
# An editor idle this long has ended a session. A bot editing 24/7 with gaps always under
# SESSION_GAP never closes its session, so it never reaches silver.editor_sessions.
SESSION_GAP = "20 minutes"
# Event-time watermark for silver dedup and sessions: the newest event_time seen (across all
# wikis and partitions) minus WATERMARK, as of the previous micro-batch. Events behind it are
# dropped from silver.edits by the dedup step, and therefore from sessions and gold; bronze
# keeps them, and ops.stream_progress counts them in rows_dropped_by_watermark. Catch-up after
# downtime with skewed partitions, or a re-emitted old event, can hit this.
WATERMARK = "10 minutes"


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
    # meta.dt is ISO-8601, normally with milliseconds and a Z suffix (2026-10-31T15:03:16.404Z).
    # The pattern-less parse is the ISO cast: it also accepts microseconds and +00:00 offsets,
    # and the try_ variant yields NULL (row dropped below) instead of raising, so one odd value
    # can never fail the query.
    return F.try_to_timestamp(dt)


def bronze_to_edits(bronze_df: DataFrame) -> DataFrame:
    """Typed edit events (types in EDIT_TYPES only). The caller deduplicates (needs a watermark).

    Rows with malformed JSON, no event id or an unparseable time are dropped here as well.
    """
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
        # Micro-batch start time, before the write and commit: not when readers can see it.
        F.current_timestamp().alias("processed_at"),
    ).where(F.col("event_id").isNotNull() & F.col("event_time").isNotNull()
            & F.col("event_type").isin(*EDIT_TYPES))


def deduplicate_edits(edits_df: DataFrame) -> DataFrame:
    """Drop repeats of the same Wikimedia event (streaming only: it needs a watermark).

    The producer resumes with Last-Event-ID and may resend a few events; they carry the
    same meta.id, so keeping the first within the watermark makes silver exactly-once per
    event, while state stays bounded to the watermark window. Side effect: the watermark
    also drops every event more than WATERMARK late from silver (see WATERMARK).
    """
    return edits_df.withWatermark("event_time", WATERMARK).dropDuplicatesWithinWatermark(
        ["event_id"])


def edits_to_sessions(edits_df: DataFrame) -> DataFrame:
    """One row per editor session on a wiki: a burst of edits with gaps under SESSION_GAP.

    Streaming: a session is emitted (append mode) once the watermark passes its window end,
    i.e. last edit + SESSION_GAP + WATERMARK; edits behind the watermark are dropped and
    counted. The watermark is a no-op on batch input.
    session_end is the time of the session's last edit, so end - start is the active time
    (a one-edit session lasts 0). `pages` is approximate (HyperLogLog, ~5% error).
    """
    window = F.session_window("event_time", SESSION_GAP)
    watermarked = edits_df.withWatermark("event_time", WATERMARK)
    sessions = watermarked.groupBy(window.alias("session"), "wiki", "user").agg(
        F.count(F.lit(1)).alias("edits"),
        F.max("event_time").alias("last_event_time"),
        F.sum(F.abs(F.coalesce("length_delta", F.lit(0)))).alias("bytes_changed"),
        F.approx_count_distinct("title").alias("pages"),
        F.max(F.col("is_bot").cast("int")).cast("boolean").alias("is_bot"),
    )
    return sessions.select(
        "wiki", "user",
        F.col("session.start").alias("session_start"),
        F.col("last_event_time").alias("session_end"),
        "edits", "bytes_changed", "pages", "is_bot",
        F.current_timestamp().alias("processed_at"),
    )
