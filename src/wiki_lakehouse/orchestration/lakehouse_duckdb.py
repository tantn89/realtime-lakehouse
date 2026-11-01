"""A DuckDB connection with the Iceberg lakehouse attached as `lake` (read path for checks).

Mirrors dbt/macros/attach_lakehouse.sql: locally the REST catalog with static keys and a
path-style endpoint; on AWS the Glue Data Catalog's Iceberg REST endpoint with the instance
credential chain.
"""

import os

import duckdb


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("LOAD httpfs; LOAD iceberg; SET TimeZone = 'UTC';")
    if os.environ.get("LAKEHOUSE_CATALOG_TYPE", "rest") == "glue":
        con.execute("LOAD aws")
        con.execute("CREATE SECRET lakehouse_s3 (TYPE S3, PROVIDER credential_chain, REGION ?)",
                    [os.environ["AWS_REGION"]])
        account = os.environ["LAKEHOUSE_GLUE_ACCOUNT_ID"]
        con.execute(f"ATTACH '{account}' AS lake (TYPE iceberg, ENDPOINT_TYPE 'glue')")
        return con
    endpoint = os.environ.get("LAKEHOUSE_S3_ENDPOINT", "")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    con.execute(
        "CREATE SECRET lakehouse_s3 (TYPE S3, KEY_ID ?, SECRET ?, REGION ?, ENDPOINT ?, "
        "URL_STYLE 'path', USE_SSL false)",
        [os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"],
         os.environ.get("AWS_REGION", "us-east-1"), endpoint],
    )
    catalog = os.environ.get("LAKEHOUSE_CATALOG_URI", "http://localhost:8181")
    con.execute(f"ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT '{catalog}', "
                "AUTHORIZATION_TYPE 'none')")
    return con
