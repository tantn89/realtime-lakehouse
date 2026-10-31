"""Run the streaming pipeline until stopped.

    python -m wiki_lakehouse.streaming
"""

from __future__ import annotations

import logging
import time

from wiki_lakehouse.spark_session import LakehouseSettings, build_spark
from wiki_lakehouse.streaming.lakehouse_tables import create_tables
from wiki_lakehouse.streaming.streaming_queries import (
    record_progress,
    start_bronze,
    start_sessions,
    start_silver,
)

log = logging.getLogger("wiki_lakehouse.streaming")
PROGRESS_EVERY_S = 30


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = LakehouseSettings.from_env()
    spark = build_spark("wiki-streaming", settings, extra_conf={
        # Session windows keep per-editor state; RocksDB keeps it off the JVM heap.
        "spark.sql.streaming.stateStore.providerClass":
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    })
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)
    queries = [start_bronze(spark, settings.kafka_bootstrap), start_silver(spark),
               start_sessions(spark)]
    log.info("started queries: %s", [q.name for q in queries])
    last_batch: dict[str, int] = {}
    try:
        while all(q.isActive for q in queries):
            time.sleep(PROGRESS_EVERY_S)
            written = record_progress(spark, queries, last_batch)
            log.info("progress rows written: %d, last batches: %s", written, last_batch)
        failed = [q for q in queries if q.exception()]
        for q in failed:
            log.error("query %s failed: %s", q.name, q.exception())
        raise SystemExit(1 if failed else 0)
    finally:
        for q in queries:
            if q.isActive:
                q.stop()


if __name__ == "__main__":
    main()
