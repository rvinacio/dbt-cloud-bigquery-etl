{% macro timestamp_brasilia() %}
    timestamp_add(current_timestamp(), interval - 3 hour)
{% endmacro %}