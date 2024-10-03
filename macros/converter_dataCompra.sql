{% macro converter_dataCompra(data) %}

    safe_cast(regexp_replace(data, r'(\d{2})/(\d{2})/(\d{4})', '\\3-\\2-\\1') as date)

{% endmacro %}