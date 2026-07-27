{#
  Standart dbt schema nomiga target prefiksini qo'shadi (masalan "analytics_staging").
  Bu override — model'ning +schema qiymatini TO'G'RIDAN-TO'G'RI schema nomi qiladi
  (masalan "staging", "marts"). Toza, o'qishli schema nomlari uchun.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
