{#
  Attach the Iceberg lakehouse as `lake` before any model runs.
  Local: static keys and a path-style endpoint for the S3-compatible store.
  AWS: no keys in the environment, so DuckDB uses the instance's credential chain and no
  secret ever appears in dbt's logs.
#}
{% macro attach_lakehouse() %}
  {%- set endpoint = env_var('LAKEHOUSE_S3_ENDPOINT', '') | replace('http://', '') | replace('https://', '') -%}
  {%- if endpoint -%}
    CREATE OR REPLACE SECRET lakehouse_s3 (
      TYPE S3, KEY_ID '{{ env_var("AWS_ACCESS_KEY_ID") }}',
      SECRET '{{ env_var("AWS_SECRET_ACCESS_KEY") }}', REGION '{{ env_var("AWS_REGION", "us-east-1") }}',
      ENDPOINT '{{ endpoint }}', URL_STYLE 'path', USE_SSL false
    );
  {%- else -%}
    CREATE OR REPLACE SECRET lakehouse_s3 (TYPE S3, PROVIDER credential_chain);
  {%- endif %}
  ATTACH IF NOT EXISTS 'warehouse' AS lake (
    TYPE iceberg, ENDPOINT '{{ env_var("LAKEHOUSE_CATALOG_URI", "http://localhost:8181") }}',
    AUTHORIZATION_TYPE 'none'
  );
{% endmacro %}
