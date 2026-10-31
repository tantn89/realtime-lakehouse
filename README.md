# realtime-lakehouse

Streaming lakehouse over live Wikipedia edits: Redpanda, Spark Structured Streaming,
Apache Iceberg, dbt and Dagster, runnable locally with one command and on AWS with Terraform.

Work in progress.

```bash
docker compose up -d --build
docker compose run --rm spark python -m wiki_lakehouse.smoke_check
```
