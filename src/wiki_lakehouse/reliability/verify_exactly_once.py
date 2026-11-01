"""Verify the lakehouse after a scenario run, including a crash mid-stream.

    python -m wiki_lakehouse.reliability.verify_exactly_once \
        --expect-kafka 20500 --expect-silver 9800 --expect-dlq 25 --late-prefix late-
    python -m wiki_lakehouse.reliability.verify_exactly_once --counts-only

Waits until silver reaches the expected count (or times out, reported as
`silver_wait_timed_out`), then checks:
- every Kafka offset of the main topic is in bronze exactly once (no loss, no duplicates);
- silver has each unique on-time edit/new event exactly once (resent duplicates collapsed;
  silver keeps only `edit` and `new`, so the expectation counts only those types);
- no event later than the watermark reached silver;
- every contract violation is in the dead-letter topic.
Prints a JSON report; exits 1 if any check fails.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

from pyspark.sql import SparkSession

from wiki_lakehouse.spark_session import LakehouseSettings, build_spark
from wiki_lakehouse.streaming.lakehouse_tables import BRONZE, SILVER_EDITS


def _kafka_count(spark: SparkSession, topic: str) -> int:
    return int(
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", LakehouseSettings.from_env().kafka_bootstrap)
        .option("subscribe", topic).option("startingOffsets", "earliest")
        .option("endingOffsets", "latest").load().count()
    )


def _scalar(spark: SparkSession, sql: str) -> Any:
    return spark.sql(sql).collect()[0][0]


def wait_for_silver(
    spark: SparkSession, expected: int, timeout_s: int
) -> tuple[int, float, bool]:
    """Poll silver until it has `expected` rows; returns (count, seconds waited, timed out)."""
    started = time.monotonic()
    while True:
        count = int(_scalar(spark, f"SELECT count(*) FROM {SILVER_EDITS}"))
        waited = time.monotonic() - started
        if count >= expected:
            return count, waited, False
        if waited > timeout_s:
            return count, waited, True
        time.sleep(5)


def main() -> None:
    parser = argparse.ArgumentParser()
    # Required unless --counts-only (checked after parsing).
    parser.add_argument("--expect-kafka", type=int, help="Messages expected in the main topic.")
    parser.add_argument("--expect-silver", type=int, help="Unique edit/new events in silver.")
    parser.add_argument("--expect-dlq", type=int, help="Messages expected in the DLQ topic.")
    parser.add_argument("--late-prefix", default="late-")
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("VERIFY_TIMEOUT", 600)))
    parser.add_argument("--counts-only", action="store_true",
                        help="Print committed bronze/silver row counts and exit (used at crash).")
    args = parser.parse_args()
    expectations = (args.expect_kafka, args.expect_silver, args.expect_dlq)
    if not args.counts_only and any(e is None for e in expectations):
        parser.error("--expect-kafka, --expect-silver and --expect-dlq are required "
                     "unless --counts-only")

    spark = build_spark("verify-exactly-once")
    spark.sparkContext.setLogLevel("ERROR")
    if args.counts_only:
        print("COUNTS " + json.dumps({
            "bronze_rows": int(_scalar(spark, f"SELECT count(*) FROM {BRONZE}")),
            "silver_rows": int(_scalar(spark, f"SELECT count(*) FROM {SILVER_EDITS}"))}))
        spark.stop()
        return
    _, waited, timed_out = wait_for_silver(spark, args.expect_silver, args.timeout)
    # One more trigger interval so nothing still in flight is missed by the checks below.
    time.sleep(15)
    observed = {
        "kafka_main_messages": _kafka_count(spark, "wiki.recentchange"),
        "kafka_dlq_messages": _kafka_count(spark, "wiki.recentchange.dlq"),
        "bronze_rows": int(_scalar(spark, f"SELECT count(*) FROM {BRONZE}")),
        "bronze_distinct_offsets": int(_scalar(
            spark, f"SELECT count(DISTINCT kafka_partition, kafka_offset) FROM {BRONZE}")),
        "silver_rows": int(_scalar(spark, f"SELECT count(*) FROM {SILVER_EDITS}")),
        "silver_distinct_events": int(_scalar(
            spark, f"SELECT count(DISTINCT event_id) FROM {SILVER_EDITS}")),
        "late_events_in_silver": int(_scalar(
            spark, f"SELECT count(*) FROM {SILVER_EDITS} "
                   f"WHERE event_id LIKE '{args.late_prefix}%'")),
        "bronze_commits": int(_scalar(spark, f"SELECT count(*) FROM {BRONZE}.snapshots")),
        "seconds_waited_for_silver": round(waited, 1),
        "silver_wait_timed_out": timed_out,
    }
    checks = {
        "kafka_has_expected_messages": observed["kafka_main_messages"] == args.expect_kafka,
        "bronze_has_every_offset_once": observed["bronze_rows"] == observed["kafka_main_messages"]
        == observed["bronze_distinct_offsets"],
        "silver_has_every_event_once": observed["silver_rows"] == args.expect_silver
        == observed["silver_distinct_events"],
        "late_events_dropped": observed["late_events_in_silver"] == 0,
        "contract_violations_in_dlq": observed["kafka_dlq_messages"] == args.expect_dlq,
    }
    report = {"passed": all(checks.values()), "checks": checks, "observed": observed,
              "expected": {"kafka": args.expect_kafka, "silver": args.expect_silver,
                           "dlq": args.expect_dlq}}
    print("RESULT " + json.dumps(report))
    spark.stop()
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
