"""Transformation tests on a local SparkSession (run inside the Spark image: needs Java).

    docker compose run --rm --no-deps -v ./tests:/app/tests \
        -v ./data/recordings:/app/data/recordings spark pytest -q tests/test_edit_transforms.py
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from wiki_lakehouse.streaming.edit_transforms import (  # noqa: E402
    EDIT_TYPES,
    bronze_to_edits,
    deduplicate_edits,
    edits_to_sessions,
)
from wiki_lakehouse.streaming.streaming_queries import progress_rows  # noqa: E402

SAMPLE = Path(__file__).parents[1] / "data" / "recordings" / "sample.jsonl"
BRONZE_SCHEMA = "value string, kafka_partition int, kafka_offset bigint, ingested_at timestamp"
EDITS_SCHEMA = ("wiki string, user string, event_time timestamp, length_delta bigint, "
                "title string, is_bot boolean")


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.streaming.stateStore.providerClass",  # same as production
                       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()


@pytest.fixture(scope="module")
def lines() -> list[str]:
    return SAMPLE.read_text(encoding="utf-8").splitlines()


def first_of(lines: list[str], event_type: str) -> str:
    return next(v for v in lines if json.loads(v)["type"] == event_type)


def retimed(line: str, event_id: str, dt: str) -> str:
    """A recorded event given a new id and event time (meta.dt)."""
    raw = json.loads(line)
    return json.dumps({**raw, "meta": {**raw["meta"], "id": event_id, "dt": dt}})


def bronze(spark: SparkSession, values: list[str]) -> DataFrame:
    rows = [(v, 0, i, datetime(2026, 1, 1)) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def file_stream(spark: SparkSession, folder: Path, batches: list[list[dict[str, Any]]],
                schema: str) -> DataFrame:
    """A JSON file stream that delivers each batch as its own micro-batch, in order."""
    folder.mkdir()
    for i, batch in enumerate(batches):
        path = folder / f"batch-{i}.json"
        path.write_text("\n".join(map(json.dumps, batch)), encoding="utf-8")
        os.utime(path, (1_700_000_000 + i, 1_700_000_000 + i))  # file source orders by mtime
    return spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(str(folder))


def bronze_rows(values: list[str]) -> list[dict[str, Any]]:
    return [{"value": v, "kafka_partition": 0, "kafka_offset": i,
             "ingested_at": "2026-01-01T00:00:00Z"} for i, v in enumerate(values)]


def run_to_memory(df: DataFrame, name: str, checkpoint: Path) -> list[dict[str, Any]]:
    query = (df.writeStream.format("memory").queryName(name).outputMode("append")
             .option("checkpointLocation", str(checkpoint)).trigger(availableNow=True).start())
    query.awaitTermination()
    return list(query.recentProgress)


def test_real_edits_parse_into_typed_rows(spark: SparkSession, lines: list[str]) -> None:
    edits = bronze_to_edits(bronze(spark, lines)).collect()
    raws = [json.loads(v) for v in lines]
    assert len(edits) == sum(r["type"] in EDIT_TYPES for r in raws)
    first, raw = edits[0], next(r for r in raws if r["type"] in EDIT_TYPES)
    assert first.event_id == raw["meta"]["id"]
    assert first.wiki == raw["wiki"] and first.is_bot == raw["bot"]
    # 2026-10-31T15:03:16.404Z -> a UTC timestamp with milliseconds kept.
    assert first.event_time.isoformat().startswith(raw["meta"]["dt"][:23])
    edit = next(e for e in edits if e.event_type == "edit")
    assert edit.length_delta == edit.length_new - edit.length_old
    assert edit.revision_new is not None


def test_categorize_and_log_events_stay_out_of_silver(
    spark: SparkSession, lines: list[str]
) -> None:
    values = [first_of(lines, t) for t in ("edit", "new", "categorize", "log")]
    kept = bronze_to_edits(bronze(spark, values)).collect()
    assert sorted(e.event_type for e in kept) == ["edit", "new"]


def test_unparseable_or_untimed_events_are_dropped(spark: SparkSession, lines: list[str]) -> None:
    good = first_of(lines, "edit")
    no_time = retimed(good, "x", "not a date")
    assert bronze_to_edits(bronze(spark, [good, "{broken", no_time])).count() == 1


def test_iso_8601_variants_parse_to_the_same_utc_instant(
    spark: SparkSession, lines: list[str]
) -> None:
    good = first_of(lines, "edit")
    variants = {"z": "2026-10-31T15:03:16.404Z", "micros": "2026-10-31T15:03:16.404000Z",
                "offset": "2026-10-31T15:03:16.404+00:00", "plus7": "2026-10-31T22:03:16.404+07:00"}
    edits = bronze_to_edits(bronze(spark, [retimed(good, k, v) for k, v in variants.items()]))
    times = {e.event_id: e.event_time for e in edits.collect()}
    assert times == dict.fromkeys(variants, datetime(2026, 10, 31, 15, 3, 16, 404000))


def test_duplicates_from_a_producer_resume_collapse_to_one_row(
    spark: SparkSession, tmp_path: Path, lines: list[str]
) -> None:
    events = [v for v in lines if json.loads(v)["type"] in EDIT_TYPES][:5]
    # A resume resends the last events: 5 unique events, the last 3 arriving twice,
    # in a second micro-batch.
    stream = file_stream(spark, tmp_path / "in", [bronze_rows(events), bronze_rows(events[2:])],
                         BRONZE_SCHEMA)
    run_to_memory(deduplicate_edits(bronze_to_edits(stream)), "deduped", tmp_path / "chk")
    assert spark.table("deduped").count() == 5
    assert spark.table("deduped").select("event_id").distinct().count() == 5


def test_an_event_later_than_the_watermark_is_dropped_from_silver_and_counted(
    spark: SparkSession, tmp_path: Path, lines: list[str]
) -> None:
    edit = first_of(lines, "edit")
    # After the first batch the watermark is 10:20 (newest 10:30 - 10 min). Spark filters late
    # rows against the previous batch's watermark, so the late event comes one batch later.
    batches = [[retimed(edit, "a", "2026-01-01T10:00:00Z"),
                retimed(edit, "b", "2026-01-01T10:30:00Z")],
               [retimed(edit, "c", "2026-01-01T10:31:00Z")],
               [retimed(edit, "late", "2026-01-01T10:05:00Z")]]
    stream = file_stream(spark, tmp_path / "in", [bronze_rows(b) for b in batches],
                         BRONZE_SCHEMA)
    progress = run_to_memory(deduplicate_edits(bronze_to_edits(stream)), "late_cut",
                             tmp_path / "chk")
    assert {r.event_id for r in spark.table("late_cut").collect()} == {"a", "b", "c"}
    assert sum(r["rows_dropped_by_watermark"] for r in progress_rows(progress, -1)) == 1


def test_sessions_split_on_a_gap_longer_than_twenty_minutes(spark: SparkSession) -> None:
    rows = [
        ("enwiki", "Ada", datetime(2026, 1, 1, 10, 0), 100, "A", False),
        ("enwiki", "Ada", datetime(2026, 1, 1, 10, 5), -40, "B", False),
        ("enwiki", "Ada", datetime(2026, 1, 1, 11, 0), 10, "A", False),  # 55 min later
        ("enwiki", "Bot1", datetime(2026, 1, 1, 10, 0), 5, "C", True),
        ("enwiki", "Cy", datetime(2026, 1, 1, 10, 0), None, "D", False),  # no length info
    ]
    edits = spark.createDataFrame(rows, EDITS_SCHEMA)
    sessions = sorted(edits_to_sessions(edits).collect(), key=lambda r: (r.user, r.session_start))
    assert [(s.user, s.edits, s.bytes_changed, s.pages) for s in sessions] == [
        ("Ada", 2, 140, 2), ("Ada", 1, 10, 1), ("Bot1", 1, 5, 1), ("Cy", 1, 0, 1)]
    assert sessions[0].session_end == datetime(2026, 1, 1, 10, 5)  # the last edit
    assert sessions[1].session_end == sessions[1].session_start  # one edit: zero length
    assert sessions[2].is_bot is True


def test_a_streaming_session_is_emitted_once_the_watermark_passes_it(
    spark: SparkSession, tmp_path: Path
) -> None:
    def edit(user: str, minute: int) -> dict[str, Any]:
        return {"wiki": "enwiki", "user": user, "event_time": f"2026-01-01T10:{minute:02d}:00Z",
                "length_delta": 1, "title": "T", "is_bot": False}

    # Ada's session closes at 10:05 + 20 min gap = 10:25; Bob's events push the watermark to
    # 10:31 (newest 10:41 - 10 min), past Ada's close but far from Bob's own (11:01).
    # Cy's edit at 10:10 arrives a batch after the watermark reached 10:30: dropped and counted.
    batches = [[edit("Ada", 0), edit("Ada", 5)], [edit("Bob", 40)], [edit("Bob", 41)],
               [edit("Cy", 10)]]
    stream = file_stream(spark, tmp_path / "in", batches, EDITS_SCHEMA)
    progress = run_to_memory(edits_to_sessions(stream), "sessions_out", tmp_path / "chk")
    emitted = spark.table("sessions_out").collect()
    assert [(s.user, s.edits, s.session_start, s.session_end) for s in emitted] == [
        ("Ada", 2, datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 10, 5))]
    assert sum(r["rows_dropped_by_watermark"] for r in progress_rows(progress, -1)) == 1


def test_progress_rows_flattens_new_batches_only() -> None:
    def progress(batch_id: int, dropped: int) -> dict[str, Any]:
        return {"name": "silver", "batchId": batch_id, "timestamp": "2026-01-01T10:00:00.500Z",
                "numInputRows": 7, "inputRowsPerSecond": None, "processedRowsPerSecond": 3.5,
                "durationMs": {"triggerExecution": 1200},
                "stateOperators": [{"numRowsDroppedByWatermark": dropped, "numRowsTotal": 4},
                                   {"numRowsDroppedByWatermark": 1, "numRowsTotal": 6}]}

    rows = progress_rows([progress(3, 0), progress(4, 2), {**progress(5, 0),
                                                           "stateOperators": None}], 3)
    assert [r["batch_id"] for r in rows] == [4, 5]
    assert rows[0] == {
        "query_name": "silver", "batch_id": 4,
        "batch_time": datetime(2026, 1, 1, 10, 0, 0, 500000), "num_input_rows": 7,
        "input_rows_per_second": 0.0, "processed_rows_per_second": 3.5,
        "batch_duration_ms": 1200, "rows_dropped_by_watermark": 3, "state_rows": 10}
    assert rows[1]["rows_dropped_by_watermark"] == 0 and rows[1]["state_rows"] == 0
