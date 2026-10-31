"""End-to-end check of the local stack: write an Iceberg table through the catalog, read it back.

    docker compose run --rm spark python -m wiki_lakehouse.smoke_check
"""

from __future__ import annotations

from wiki_lakehouse.spark_session import build_spark


def main() -> None:
    spark = build_spark("smoke-check")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ops")
    spark.sql("CREATE TABLE IF NOT EXISTS ops.smoke (id INT, note STRING) USING iceberg")
    spark.sql("INSERT INTO ops.smoke VALUES (1, 'written by spark')")
    rows = spark.sql("SELECT count(*) AS n FROM ops.smoke").collect()[0]["n"]
    snapshots = spark.sql("SELECT count(*) AS n FROM ops.smoke.snapshots").collect()[0]["n"]
    print(f"ops.smoke rows={rows} snapshots={snapshots}")
    spark.stop()


if __name__ == "__main__":
    main()
