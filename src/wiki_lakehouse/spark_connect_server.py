"""Start a Spark Connect server with the lakehouse catalog configured.

Dagster submits Iceberg maintenance SQL through it, so the orchestrator needs only the thin
Spark Connect client, not a JVM.

    python -m wiki_lakehouse.spark_connect_server
"""

from __future__ import annotations

import os
from pathlib import Path

import pyspark

from wiki_lakehouse.spark_session import LakehouseSettings, iceberg_conf

PORT = "15002"


def command() -> list[str]:
    spark_submit = Path(pyspark.__file__).parent / "bin" / "spark-submit"
    conf = {
        **iceberg_conf(LakehouseSettings.from_env()),
        "spark.connect.grpc.binding.port": PORT,
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.memory": os.environ.get("SPARK_CONNECT_DRIVER_MEMORY", "2g"),
    }
    args = [str(spark_submit), "--class", "org.apache.spark.sql.connect.service.SparkConnectServer",
            "--name", "lakehouse-spark-connect"]
    for key, value in conf.items():
        args += ["--conf", f"{key}={value}"]
    # spark-submit needs a primary resource; the Connect server class lives in a jar we ship.
    jars = Path(pyspark.__file__).parent / "jars"
    return [*args, str(next(jars.glob("spark-connect_*.jar")))]


def main() -> None:
    args = command()
    os.execv(args[0], args)


if __name__ == "__main__":
    main()
