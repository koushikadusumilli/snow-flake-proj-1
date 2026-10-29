{#-
    By default dbt builds schema names as <target schema>_<custom schema>, which
    would turn `staging` into `analytics_staging`. This override uses the custom
    schema exactly as configured, and falls back to the target schema when a
    model has none. See:
    https://docs.getdbt.com/docs/build/custom-schemas
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
