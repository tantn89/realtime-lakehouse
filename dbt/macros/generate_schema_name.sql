{# Use the configured schema as is (gold, staging) instead of dbt's "<target>_<schema>". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name or target.schema }}
{%- endmacro %}
