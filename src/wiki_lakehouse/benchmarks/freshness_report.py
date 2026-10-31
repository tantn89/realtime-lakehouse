"""End-to-end freshness per benchmark phase, and silver compaction.

    python -m wiki_lakehouse.benchmarks.freshness_report freshness --phase b1000=60000 ...
    python -m wiki_lakehouse.benchmarks.freshness_report compact

Freshness of a row = commit time of the Iceberg snapshot that added it - its event time.
That is when a reader could first see the edit, not when Spark started the batch. Events
are re-timed to "now" at publish, so this is the pipeline's latency end to end (producer,
Kafka, bronze stream, silver stream, commit).
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
    WHERE e.event_id LIKE '{{prefix}}%'
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
                             f"WHERE event_id LIKE '{prefix}%'").collect()[0][0])
        if rows >= expected or time.monotonic() > deadline:
            return rows
        time.sleep(5)


def freshness(spark: SparkSession, phases: dict[str, int], timeout_s: int) -> None:
    for name, expected in phases.items():
        seen = _wait_for(spark, f"{name}-", expected, timeout_s)
        row = spark.sql(FRESHNESS_SQL.replace("{prefix}", f"{name}-")).collect()[0].asDict()
        print("RESULT " + json.dumps({"phase": name, "expected": expected, "seen": seen, **row}),
              flush=True)


def file_stats(spark: SparkSession) -> dict[str, float]:
    row = spark.sql(f"SELECT count(*) AS files, avg(file_size_in_bytes) AS avg_bytes "
                    f"FROM {SILVER_EDITS}.files").collect()[0]
    return {"files": int(row["files"]), "avg_kb": round(float(row["avg_bytes"] or 0) / 1024, 1)}


def compact(spark: SparkSession) -> None:
    before = file_stats(spark)
    started = time.monotonic()
    spark.sql(f"CALL lake.system.rewrite_data_files(table => '{SILVER_EDITS}', options => "
              "map('min-input-files', '2', 'target-file-size-bytes', '134217728'))").collect()
    elapsed = round(time.monotonic() - started, 1)
    print("RESULT " + json.dumps({"compaction": {"before": before, "after": file_stats(spark),
                                                 "seconds": elapsed}}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    f = sub.add_parser("freshness")
    f.add_argument("--phase", action="append", default=[],
                   help="name=expected_rows; rows have ids starting with '<name>-'")
    f.add_argument("--timeout", type=int, default=900)
    sub.add_parser("compact")
    args = parser.parse_args()

    spark = build_spark("benchmark-report")
    spark.sparkContext.setLogLevel("ERROR")
    if args.command == "freshness":
        phases = {p.split("=")[0]: int(p.split("=")[1]) for p in args.phase}
        freshness(spark, phases, args.timeout)
    else:
        compact(spark)
    spark.stop()


if __name__ == "__main__":
    main()
