"""Health checks on the streaming tables, run by Dagster every few minutes.

The stream itself never reports "I am stuck" or "upstream sent garbage"; these checks do.
"""

from __future__ import annotations

import os
from typing import Any

from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

from wiki_lakehouse.orchestration.lakehouse_duckdb import connect

SILVER_EDITS = AssetKey(["silver", "edits"])
BRONZE = AssetKey(["bronze", "recentchange_raw"])
FRESHNESS_SLA_S = 300
VOLUME_FLOOR = 0.3  # last 10 minutes vs the previous hour's 10-minute average
DLQ_MAX_SHARE = 0.01


def _one(sql: str) -> tuple[Any, ...]:
    row = connect().execute(sql).fetchone()
    assert row is not None
    return tuple(row)


@asset_check(asset=SILVER_EDITS, description=f"Newest silver row is under {FRESHNESS_SLA_S}s old.")
def silver_is_fresh() -> AssetCheckResult:
    (lag_s,) = _one("SELECT date_diff('second', max(processed_at), now()) FROM lake.silver.edits")
    return AssetCheckResult(passed=lag_s is not None and lag_s <= FRESHNESS_SLA_S,
                            metadata={"lag_seconds": lag_s})


@asset_check(asset=SILVER_EDITS, description="No event appears twice in the last hour.")
def silver_has_no_duplicates() -> AssetCheckResult:
    (dupes,) = _one("""
        SELECT count(*) - count(DISTINCT event_id) FROM lake.silver.edits
        WHERE processed_at > now() - INTERVAL 1 HOUR""")
    return AssetCheckResult(passed=dupes == 0, metadata={"duplicate_events": dupes})


@asset_check(asset=SILVER_EDITS,
             description="Edit volume has not collapsed (an upstream outage or a stuck producer).")
def silver_volume_is_normal() -> AssetCheckResult:
    recent, baseline = _one("""
        SELECT count(*) FILTER (WHERE event_time > now() - INTERVAL 10 MINUTES),
               count(*) FILTER (WHERE event_time BETWEEN now() - INTERVAL 70 MINUTES
                                                     AND now() - INTERVAL 10 MINUTES) / 6.0
        FROM lake.silver.edits WHERE event_time > now() - INTERVAL 70 MINUTES""")
    if not baseline:
        return AssetCheckResult(passed=True, metadata={"note": "no baseline hour yet"})
    ratio = recent / baseline
    return AssetCheckResult(passed=ratio >= VOLUME_FLOOR, severity=AssetCheckSeverity.WARN,
                            metadata={"last_10_min": recent, "baseline_10_min": baseline,
                                      "ratio": round(ratio, 3)})


@asset_check(asset=BRONZE, description=f"Under {DLQ_MAX_SHARE:.0%} of events break the contract.")
def contract_violations_are_rare() -> AssetCheckResult:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": os.environ["LAKEHOUSE_KAFKA_BOOTSTRAP"],
                         "group.id": "lakehouse-checks", "enable.auto.commit": False})
    try:
        def total(topic: str) -> int:
            metadata = consumer.list_topics(topic, timeout=10).topics[topic]
            return sum(consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                       for p in metadata.partitions)

        main, dlq = total("wiki.recentchange"), total("wiki.recentchange.dlq")
    finally:
        consumer.close()
    share = dlq / (main + dlq) if main + dlq else 0.0
    return AssetCheckResult(passed=share < DLQ_MAX_SHARE,
                            metadata={"main_events": main, "dlq_events": dlq,
                                      "dlq_share": round(share, 5)})


CHECKS = [silver_is_fresh, silver_has_no_duplicates, silver_volume_is_normal,
          contract_violations_are_rare]
