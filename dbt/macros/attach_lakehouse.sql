{#
  Attach the Iceberg lakehouse as `lake` before any model runs.
  Local: REST catalog, static keys and a path-style endpoint for the S3-compatible store.
  AWS: the Glue Data Catalog's Iceberg REST endpoint (SigV4), with credentials from the
  instance's credential chain, so no secret ever appears in dbt's logs.
  Keep in sync with src/wiki_lakehouse/orchestration/lakehouse_duckdb.py.
#}
{% macro attach_lakehouse() %}
  {%- if env_var('LAKEHOUSE_CATALOG_TYPE', 'rest') == 'glue' -%}
    CREATE OR REPLACE SECRET lakehouse_s3 (
      TYPE S3, PROVIDER credential_chain, REGION '{{ env_var("AWS_REGION") }}'
    );
    ATTACH IF NOT EXISTS '{{ env_var("LAKEHOUSE_GLUE_ACCOUNT_ID") }}' AS lake (
      TYPE iceberg, ENDPOINT_TYPE 'glue'
    );
  {%- else -%}
    {%- set endpoint = env_var('LAKEHOUSE_S3_ENDPOINT', '') | replace('http://', '') | replace('https://', '') -%}
    CREATE OR REPLACE SECRET lakehouse_s3 (
      TYPE S3, KEY_ID '{{ env_var("AWS_ACCESS_KEY_ID", "") }}',
      SECRET '{{ env_var("AWS_SECRET_ACCESS_KEY", "") }}', REGION '{{ env_var("AWS_REGION", "us-east-1") }}',
      ENDPOINT '{{ endpoint }}', URL_STYLE 'path', USE_SSL false
    );
    ATTACH IF NOT EXISTS 'warehouse' AS lake (
      TYPE iceberg, ENDPOINT '{{ env_var("LAKEHOUSE_CATALOG_URI", "http://localhost:8181") }}',
      AUTHORIZATION_TYPE 'none'
    );
  {%- endif %}
{% endmacro %}
