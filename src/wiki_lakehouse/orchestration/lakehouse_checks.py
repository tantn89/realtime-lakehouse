"""Health checks on the streaming tables, run by Dagster every few minutes.

The stream itself never reports "I am stuck" or "upstream sent garbage"; these checks do.
"""

# No `from __future__ import annotations`: Dagster inspects real annotations.
import os
import time
from typing import Any

from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

from wiki_lakehouse.orchestration.lakehouse_duckdb import connect

SILVER_EDITS = AssetKey(["silver", "edits"])
BRONZE = AssetKey(["bronze", "recentchange_raw"])
FRESHNESS_SLA_S = 300
VOLUME_FLOOR = 0.3  # last 10 minutes vs the previous hour's 10-minute average
DLQ_MAX_SHARE = 0.01
DLQ_WINDOW_S = 3600
# Silver is partitioned by days(event_time); filtering on processed_at alone scans every
# partition. The stream's 10-minute watermark drops older events, so any row processed in
# the last hour has an event_time inside the last day.
RECENT_PARTITIONS = "event_time > now() - INTERVAL 1 DAY"


def _one(sql: str) -> tuple[Any, ...]:
    row = connect().execute(sql).fetchone()
    assert row is not None
    return tuple(row)


@asset_check(asset=SILVER_EDITS, description=f"Newest silver row is under {FRESHNESS_SLA_S}s old.")
def silver_is_fresh() -> AssetCheckResult:
    # No row in the last day of partitions reads as NULL lag, which fails the check.
    (lag_s,) = _one(f"""
        SELECT date_diff('second', max(processed_at), now()) FROM lake.silver.edits
        WHERE {RECENT_PARTITIONS}""")
    return AssetCheckResult(passed=lag_s is not None and lag_s <= FRESHNESS_SLA_S,
                            metadata={"lag_seconds": lag_s})


@asset_check(asset=SILVER_EDITS, description="No event appears twice in the last hour.")
def silver_has_no_duplicates() -> AssetCheckResult:
    # A duplicate shares its event_time with the original, so the partition filter cannot
    # split a pair.
    (dupes,) = _one(f"""
        SELECT count(*) - count(DISTINCT event_id) FROM lake.silver.edits
        WHERE processed_at > now() - INTERVAL 1 HOUR AND {RECENT_PARTITIONS}""")
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


def _messages_since(consumer: Any, topic: str, since_ms: int) -> int:
    """Messages appended to `topic` at or after `since_ms` (epoch ms), over all partitions.

    offsets_for_times gives each partition's first offset at that time (-1: none yet, so the
    window starts at the end); the low watermark clamps it to what retention still holds.
    """
    from confluent_kafka import KafkaException, TopicPartition

    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    starts = consumer.offsets_for_times(
        [TopicPartition(topic, p, since_ms) for p in partitions], timeout=10)
    total = 0
    for start in starts:
        if start.error is not None:
            raise KafkaException(start.error)
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, start.partition), timeout=10)
        total += high - (high if start.offset < 0 else max(start.offset, low))
    return total


@asset_check(asset=BRONZE, description=f"Under {DLQ_MAX_SHARE:.0%} of the last hour's events "
                                       "break the contract.")
def contract_violations_are_rare() -> AssetCheckResult:
    from confluent_kafka import Consumer

    since_ms = int(time.time() * 1000) - DLQ_WINDOW_S * 1000
    consumer = Consumer({"bootstrap.servers": os.environ["LAKEHOUSE_KAFKA_BOOTSTRAP"],
                         "group.id": "lakehouse-checks", "enable.auto.commit": False})
    try:
        main = _messages_since(consumer, "wiki.recentchange", since_ms)
        dlq = _messages_since(consumer, "wiki.recentchange.dlq", since_ms)
    finally:
        consumer.close()
    share = dlq / (main + dlq) if main + dlq else 0.0
    return AssetCheckResult(passed=share < DLQ_MAX_SHARE,
                            metadata={"window_seconds": DLQ_WINDOW_S, "main_events": main,
                                      "dlq_events": dlq, "dlq_share": round(share, 5)})


CHECKS = [silver_is_fresh, silver_has_no_duplicates, silver_volume_is_normal,
          contract_violations_are_rare]
