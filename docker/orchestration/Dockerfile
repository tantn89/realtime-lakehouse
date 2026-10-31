# dbt (on DuckDB, reading the Iceberg lakehouse) and Dagster in one image. Spark is reached
# through Spark Connect, so this image has no JVM.
FROM python:3.11-slim-bookworm

RUN useradd --create-home --uid 1000 lakehouse
WORKDIR /app
RUN pip install --no-cache-dir \
      duckdb==2.0.0 pytz==2026.6 dbt-core==1.12.6 dbt-duckdb==1.11.1 \
      dagster==1.13.28 dagster-webserver==1.13.28 dagster-dbt==0.29.28 \
      "pyspark[connect]==3.5.3" \
      # pyarrow 18+ needs NumPy 2; another dependency here still pins NumPy 1.26.
      pyarrow==17.0.0

COPY pyproject.toml README.md ./
COPY src ./src
COPY dbt ./dbt
RUN pip install --no-cache-dir ".[producer]" \
 && mkdir -p /gold /dagster_home && chown -R lakehouse /app /gold /dagster_home

USER lakehouse
ENV DBT_PROFILES_DIR=/app/dbt DBT_PROJECT_DIR=/app/dbt DAGSTER_HOME=/dagster_home \
    LAKEHOUSE_GOLD_DB=/gold/lakehouse.duckdb
# Bake the DuckDB extensions and the dbt manifest Dagster reads into the image.
RUN python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('httpfs', 'iceberg')]" \
 && cd dbt && dbt parse --quiet
