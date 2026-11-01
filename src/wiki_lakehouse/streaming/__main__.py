"""Run the streaming pipeline until stopped (SIGTERM / SIGINT stop the queries cleanly).

    python -m wiki_lakehouse.streaming
"""

from __future__ import annotations

import logging
import signal
import time
from types import FrameType

from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

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
# How often the loop wakes to check for a stop signal. `docker stop` waits 10 s before
# SIGKILL, so this plus stopping the queries must fit in that grace period.
POLL_S = 3


class _StopFlag:
    """Set by the signal handler; the main loop polls it (handlers must not block on Spark)."""

    def __init__(self) -> None:
        self.signum: int | None = None

    def __call__(self, signum: int, _frame: FrameType | None) -> None:
        self.signum = signum


def _write_progress(spark: SparkSession, queries: list[StreamingQuery],
                    last_batch: dict[str, int]) -> None:
    # Observability must never stop the data path: a failed write (catalog or S3 hiccup) is
    # logged and retried on the next tick, while the queries keep running.
    try:
        written = record_progress(spark, queries, last_batch)
        log.info("progress rows written: %d, last batches: %s", written, last_batch)
    except Exception:
        log.exception("writing stream progress failed; retrying next tick")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = _StopFlag()
    # Python runs as PID 1 in the container, where an unhandled SIGTERM is ignored.
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
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
    next_progress = time.monotonic() + PROGRESS_EVERY_S
    try:
        while stop.signum is None and all(q.isActive for q in queries):
            try:
                spark.streams.awaitAnyTermination(timeout=POLL_S)
            except StreamingQueryException:
                break  # reported below
            if time.monotonic() >= next_progress:
                _write_progress(spark, queries, last_batch)
                next_progress = time.monotonic() + PROGRESS_EVERY_S
        if stop.signum is not None:
            log.info("received signal %d, stopping queries", stop.signum)
            raise SystemExit(0)
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
