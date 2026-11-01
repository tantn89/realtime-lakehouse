"""End-to-end freshness per benchmark phase, and silver compaction.

    python -m wiki_lakehouse.benchmarks.freshness_report freshness --phase b1000=60000 ...
    python -m wiki_lakehouse.benchmarks.freshness_report compact

Freshness of a row = commit time of the Iceberg snapshot that added it - its event time.
That is when a reader could first see the edit, not when Spark started the batch. Events
are re-timed to "now" at publish, so this is the pipeline's latency end to end (producer,
Kafka, bronze stream, silver stream, commit).

Valid only before compaction: rewritten rows live in files added by the compaction snapshot,
so they would report its commit time. The benchmark measures first, then compacts.
"""

from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import SparkSession

from wiki_lakehouse.spark_session import build_spark
from wiki_lakehouse.streaming.lakehouse_tables import SILVER_EDITS

FRESHNESS_SQL = f"""
WITH added AS (
    SELECT DISTINCT data_file.file_path AS file_path, snapshot_id
    FROM {SILVER_EDITS}.entries WHERE status < 2
),
latency AS (
    SELECT unix_millis(s.committed_at) - unix_millis(e.event_time) AS ms
    FROM {SILVER_EDITS} e
    JOIN added a ON e._file = a.file_path
    JOIN {SILVER_EDITS}.snapshots s ON s.snapshot_id = a.snapshot_id
    WHERE startswith(e.event_id, :prefix)
)
SELECT count(*) AS rows,
       round(percentile(ms, 0.5) / 1000, 1) AS p50_s,
       round(percentile(ms, 0.95) / 1000, 1) AS p95_s,
       round(percentile(ms, 0.99) / 1000, 1) AS p99_s,
       round(max(ms) / 1000, 1) AS max_s
FROM latency
"""


def _wait_for(spark: SparkSession, prefix: str, expected: int, timeout_s: int) -> int:
    deadline = time.monotonic() + timeout_s
    while True:
        rows = int(spark.sql(f"SELECT count(*) FROM {SILVER_EDITS} "
                             "WHERE startswith(event_id, :prefix)",
                             args={"prefix": prefix}).collect()[0][0])
        if rows >= expected or time.monotonic() > deadline:
            return rows
        time.sleep(5)


def freshness(spark: SparkSession, phases: dict[str, int], timeout_s: int) -> None:
    for name, expected in phases.items():
        prefix = f"{name}-"
        seen = _wait_for(spark, prefix, expected, timeout_s)
        row = spark.sql(FRESHNESS_SQL, args={"prefix": prefix}).collect()[0].asDict()
        print("RESULT " + json.dumps({"phase": name, "expected": expected, "seen": seen, **row}),
              flush=True)


def file_stats(spark: SparkSession) -> dict[str, float]:
    # split_offsets holds one entry per Parquet row group: the unit readers parallelise on.
    # A file without it counts as one row group; CASE because size(NULL) is -1, not NULL.
    row = spark.sql(f"SELECT count(*) AS files, avg(file_size_in_bytes) AS avg_bytes, "
                    "sum(CASE WHEN split_offsets IS NULL THEN 1 ELSE size(split_offsets) END) "
                    "AS row_groups "
                    f"FROM {SILVER_EDITS}.files").collect()[0]
    return {"files": int(row["files"]), "row_groups": int(row["row_groups"] or 0),
            "avg_kb": round(float(row["avg_bytes"] or 0) / 1024, 1)}


def compact(spark: SparkSession) -> None:
    before = file_stats(spark)
    started = time.monotonic()
    spark.sql(f"CALL lake.system.rewrite_data_files(table => '{SILVER_EDITS}', options => "
              "map('min-input-files', '2', 'target-file-size-bytes', '134217728'))").collect()
    elapsed = round(time.monotonic() - started, 1)
    print("RESULT " + json.dumps({"compaction": {"before": before, "after": file_stats(spark),
                                                 "seconds": elapsed}}), flush=True)


def parse_phase(value: str) -> tuple[str, int]:
    """argparse type for `--phase name=N`: N = silver (edit/new) rows expected for the phase."""
    name, sep, count = value.partition("=")
    if not sep or not name or not count.isdigit():
        raise argparse.ArgumentTypeError(f"expected name=N with N a row count, got {value!r}")
    return name, int(count)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    f = sub.add_parser("freshness")
    f.add_argument("--phase", action="append", default=[], type=parse_phase,
                   help="name=N: N silver (edit/new) rows expected, ids starting '<name>-'")
    f.add_argument("--timeout", type=int, default=900)
    sub.add_parser("compact")
    args = parser.parse_args()

    spark = build_spark("benchmark-report")
    spark.sparkContext.setLogLevel("ERROR")
    if args.command == "freshness":
        phases = dict(args.phase)
        freshness(spark, phases, args.timeout)
    else:
        compact(spark)
    spark.stop()


if __name__ == "__main__":
    main()
