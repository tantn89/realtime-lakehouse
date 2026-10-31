"""Print the state of the lakehouse: row counts, duplicates, freshness and stream progress.

    docker compose run --rm spark python -m wiki_lakehouse.lakehouse_status
"""

from __future__ import annotations

from wiki_lakehouse.spark_session import build_spark
from wiki_lakehouse.streaming.lakehouse_tables import (
    BRONZE,
    SILVER_EDITS,
    SILVER_SESSIONS,
    STREAM_PROGRESS,
)

STATUS_SQL = {
    "tables": f"""
        SELECT '{BRONZE}' AS table_name, count(*) AS rows FROM {BRONZE}
        UNION ALL SELECT '{SILVER_EDITS}', count(*) FROM {SILVER_EDITS}
        UNION ALL SELECT '{SILVER_SESSIONS}', count(*) FROM {SILVER_SESSIONS}""",
    # Exactly-once check: every Kafka offset lands in bronze once, every event in silver once.
    "duplicates": f"""
        SELECT
          (SELECT count(*) - count(DISTINCT kafka_partition, kafka_offset) FROM {BRONZE})
            AS bronze_duplicate_offsets,
          (SELECT count(*) - count(DISTINCT event_id) FROM {SILVER_EDITS})
            AS silver_duplicate_events""",
    # Freshness: how long after an edit happened it became queryable in silver.
    "freshness_last_hour": f"""
        SELECT count(*) AS edits,
               round(percentile(unix_millis(processed_at) - unix_millis(event_time), 0.5)
                     / 1000, 1) AS p50_s,
               round(percentile(unix_millis(processed_at) - unix_millis(event_time), 0.95)
                     / 1000, 1) AS p95_s
        FROM {SILVER_EDITS}
        WHERE processed_at > current_timestamp() - INTERVAL 1 HOUR""",
    "stream_progress_last_10_min": f"""
        SELECT query_name, count(*) AS batches, sum(num_input_rows) AS rows,
               round(avg(processed_rows_per_second), 1) AS avg_rows_per_s,
               max(batch_duration_ms) AS max_batch_ms,
               sum(rows_dropped_by_watermark) AS late_rows_dropped
        FROM {STREAM_PROGRESS}
        WHERE batch_time > current_timestamp() - INTERVAL 10 MINUTES
        GROUP BY query_name ORDER BY query_name""",
}


def main() -> None:
    spark = build_spark("lakehouse-status")
    spark.sparkContext.setLogLevel("ERROR")
    for title, sql in STATUS_SQL.items():
        print(f"\n== {title}")
        spark.sql(sql).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
