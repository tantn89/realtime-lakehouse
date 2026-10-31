"""Median latency of a typical gold-layer read on silver, from DuckDB.

    python -m wiki_lakehouse.benchmarks.duckdb_query_latency --runs 7

Run before and after compaction: the query is the same, only the file layout differs.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time

from wiki_lakehouse.orchestration.lakehouse_duckdb import connect

QUERY = """
SELECT date_trunc('minute', event_time) AS minute, wiki,
       count(*) AS edits, count(*) FILTER (WHERE is_bot) AS bot_edits
FROM lake.silver.edits
GROUP BY ALL
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    con = connect()
    con.execute(QUERY).fetchall()  # warm-up: catalog lookups, extension loading
    timings = []
    for _ in range(args.runs):
        started = time.perf_counter()
        con.execute(QUERY).fetchall()
        timings.append((time.perf_counter() - started) * 1000)
    rows = con.execute("SELECT count(*) FROM lake.silver.edits").fetchone()
    print("RESULT " + json.dumps({"duckdb_query": {
        "label": args.label, "silver_rows": rows[0] if rows else None,
        "median_ms": round(statistics.median(timings), 1), "runs": args.runs}}), flush=True)


if __name__ == "__main__":
    main()
