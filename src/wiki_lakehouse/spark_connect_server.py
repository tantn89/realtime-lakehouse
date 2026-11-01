"""Start a Spark Connect server with the lakehouse catalog configured.

Dagster submits Iceberg maintenance SQL through it, so the orchestrator needs only the thin
Spark Connect client, not a JVM.

Security: the endpoint has no authentication and runs arbitrary SQL with the catalog's
credentials. It is reachable only on the internal compose network; never publish its port.

    python -m wiki_lakehouse.spark_connect_server
"""

from __future__ import annotations

import os
from pathlib import Path

import pyspark

from wiki_lakehouse.spark_session import LakehouseSettings, iceberg_conf

PORT = "15002"


def connect_jar(jars: Path) -> Path:
    """The Spark Connect server jar the Spark image ships in pyspark's jars directory."""
    found = sorted(jars.glob("spark-connect_*.jar"))
    if not found:
        raise SystemExit(f"spark-connect_*.jar not found in {jars}: "
                         "run this in the Spark image (docker/spark/Dockerfile)")
    return found[0]


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
    return [*args, str(connect_jar(Path(pyspark.__file__).parent / "jars"))]


def main() -> None:
    args = command()
    os.execv(args[0], args)


if __name__ == "__main__":
    main()
