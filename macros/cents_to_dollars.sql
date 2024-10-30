{% macro cents_to_dollars(field) -%}
    round({{field}}/100,2)
{%- endmacro -%}