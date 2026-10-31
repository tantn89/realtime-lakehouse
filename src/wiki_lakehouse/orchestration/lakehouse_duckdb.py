"""A DuckDB connection with the Iceberg lakehouse attached as `lake` (read path for checks).

Mirrors dbt/macros/attach_lakehouse.sql: static keys and a path-style endpoint locally,
the instance credential chain on AWS.
"""

from __future__ import annotations

import os

import duckdb


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("LOAD httpfs; LOAD iceberg; SET TimeZone = 'UTC';")
    endpoint = os.environ.get("LAKEHOUSE_S3_ENDPOINT", "")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    if endpoint:
        con.execute(
            "CREATE SECRET lakehouse_s3 (TYPE S3, KEY_ID ?, SECRET ?, REGION ?, ENDPOINT ?, "
            "URL_STYLE 'path', USE_SSL false)",
            [os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"],
             os.environ.get("AWS_REGION", "us-east-1"), endpoint],
        )
    else:
        con.execute("CREATE SECRET lakehouse_s3 (TYPE S3, PROVIDER credential_chain)")
    catalog = os.environ.get("LAKEHOUSE_CATALOG_URI", "http://localhost:8181")
    con.execute(f"ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT '{catalog}', "
                "AUTHORIZATION_TYPE 'none')")
    return con
