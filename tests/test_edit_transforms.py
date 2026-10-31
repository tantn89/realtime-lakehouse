"""Transformation tests on a local SparkSession (run inside the Spark image: needs Java).

    docker compose run --rm -v ./tests:/app/tests spark pytest tests/test_edit_transforms.py
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from wiki_lakehouse.streaming.edit_transforms import (  # noqa: E402
    bronze_to_edits,
    deduplicate_edits,
    edits_to_sessions,
)

SAMPLE = Path(__file__).parents[1] / "data" / "recordings" / "sample.jsonl"


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()


def bronze(spark: SparkSession, values: list[str]) -> DataFrame:
    rows = [(v, 0, i, datetime(2026, 1, 1)) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, "value string, kafka_partition int, kafka_offset bigint, "
                                       "ingested_at timestamp")


def test_real_events_parse_into_typed_edits(spark: SparkSession) -> None:
    lines = SAMPLE.read_text(encoding="utf-8").splitlines()
    edits = bronze_to_edits(bronze(spark, lines)).collect()
    assert len(edits) == len(lines)
    first, raw = edits[0], json.loads(lines[0])
    assert first.event_id == raw["meta"]["id"]
    assert first.wiki == raw["wiki"] and first.is_bot == raw["bot"]
    # 2026-10-31T15:03:16.404Z -> a UTC timestamp with milliseconds kept.
    assert first.event_time.isoformat().startswith(raw["meta"]["dt"][:23])
    edit = next(e for e, r in zip(edits, map(json.loads, lines), strict=True)
                if r["type"] == "edit")
    assert edit.length_delta == edit.length_new - edit.length_old
    assert edit.revision_new is not None


def test_unparseable_or_untimed_events_are_dropped(spark: SparkSession) -> None:
    good = SAMPLE.read_text(encoding="utf-8").splitlines()[0]
    no_time = json.dumps({**json.loads(good), "meta": {"id": "x", "dt": "not a date"}})
    assert bronze_to_edits(bronze(spark, [good, "{broken", no_time])).count() == 1


def test_duplicates_from_a_producer_resume_collapse_to_one_row(
    spark: SparkSession, tmp_path: Path
) -> None:
    lines = SAMPLE.read_text(encoding="utf-8").splitlines()[:5]
    # A resume resends the last events: 5 unique events, the last 3 arriving twice,
    # split across two files so they land in different micro-batches.
    for name, batch in (("a.json", lines), ("b.json", lines[2:])):
        rows = [{"value": v, "kafka_partition": 0, "kafka_offset": i,
                 "ingested_at": "2026-01-01T00:00:00Z"} for i, v in enumerate(batch)]
        (tmp_path / "in").mkdir(exist_ok=True)
        (tmp_path / "in" / name).write_text("\n".join(map(json.dumps, rows)), encoding="utf-8")
    stream = spark.readStream.schema(
        "value string, kafka_partition int, kafka_offset bigint, ingested_at timestamp"
    ).option("maxFilesPerTrigger", 1).json(str(tmp_path / "in"))
    query = (deduplicate_edits(bronze_to_edits(stream)).writeStream.format("memory")
             .queryName("deduped").option("checkpointLocation", str(tmp_path / "chk"))
             .trigger(availableNow=True).start())
    query.awaitTermination()
    assert spark.table("deduped").count() == 5
    assert spark.table("deduped").select("event_id").distinct().count() == 5


def test_sessions_split_on_a_gap_longer_than_twenty_minutes(spark: SparkSession) -> None:
    rows = [
        ("enwiki", "Ada", datetime(2026, 1, 1, 10, 0), 100, "A", False),
        ("enwiki", "Ada", datetime(2026, 1, 1, 10, 5), -40, "B", False),
        ("enwiki", "Ada", datetime(2026, 1, 1, 11, 0), 10, "A", False),  # 55 min later
        ("enwiki", "Bot1", datetime(2026, 1, 1, 10, 0), 5, "C", True),
    ]
    edits = spark.createDataFrame(rows, "wiki string, user string, event_time timestamp, "
                                        "length_delta bigint, title string, is_bot boolean")
    sessions = sorted(edits_to_sessions(edits).collect(), key=lambda r: (r.user, r.session_start))
    assert [(s.user, s.edits, s.bytes_changed, s.pages) for s in sessions] == [
        ("Ada", 2, 140, 2), ("Ada", 1, 10, 1), ("Bot1", 1, 5, 1)]
    assert sessions[0].session_end == datetime(2026, 1, 1, 10, 25)  # last edit + gap
    assert sessions[2].is_bot is True
