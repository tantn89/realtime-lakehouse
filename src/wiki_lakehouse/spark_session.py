"""SparkSession wired to the Iceberg catalog, configured from LAKEHOUSE_* environment variables.

The same code runs against the local stack (REST catalog + S3-compatible SeaweedFS) and AWS
(S3 + Glue): only the environment changes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession

CATALOG = "lake"


@dataclass(frozen=True)
class LakehouseSettings:
    catalog_type: str  # "rest" (local) or "glue" (AWS)
    catalog_uri: str
    warehouse: str
    s3_endpoint: str  # empty on AWS: the default S3 endpoint
    kafka_bootstrap: str

    @classmethod
    def from_env(cls) -> LakehouseSettings:
        return cls(
            catalog_type=os.environ.get("LAKEHOUSE_CATALOG_TYPE", "rest"),
            catalog_uri=os.environ.get("LAKEHOUSE_CATALOG_URI", "http://localhost:8181"),
            warehouse=os.environ.get("LAKEHOUSE_WAREHOUSE", "s3://warehouse/"),
            s3_endpoint=os.environ.get("LAKEHOUSE_S3_ENDPOINT", ""),
            kafka_bootstrap=os.environ.get("LAKEHOUSE_KAFKA_BOOTSTRAP", "localhost:19092"),
        )


def iceberg_conf(settings: LakehouseSettings) -> dict[str, str]:
    prefix = f"spark.sql.catalog.{CATALOG}"
    conf = {
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.warehouse": settings.warehouse,
        # Iceberg caches table metadata per session and expires it after *access*, so a
        # process polling a table every few seconds never sees new commits. Other processes
        # write these tables continuously; always read the current snapshot.
        f"{prefix}.cache-enabled": "false",
        # ResolvingFileIO picks the FileIO per path scheme: S3 for table files, Hadoop for the
        # local checkpoint directory, where Iceberg's streaming source writes its offsets.
        f"{prefix}.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
        "spark.sql.defaultCatalog": CATALOG,
    }
    if settings.catalog_type == "glue":
        conf[f"{prefix}.catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
    else:
        conf[f"{prefix}.type"] = "rest"
        conf[f"{prefix}.uri"] = settings.catalog_uri
    if settings.s3_endpoint:
        conf[f"{prefix}.s3.endpoint"] = settings.s3_endpoint
        conf[f"{prefix}.s3.path-style-access"] = "true"
    return conf


def build_spark(
    app_name: str,
    settings: LakehouseSettings | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    settings = settings or LakehouseSettings.from_env()
    conf = {
        **iceberg_conf(settings),
        # Small cluster: fewer shuffle partitions than the default 200 avoids tiny files
        # and tiny state stores.
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.session.timeZone": "UTC",
        **(extra_conf or {}),
    }
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
