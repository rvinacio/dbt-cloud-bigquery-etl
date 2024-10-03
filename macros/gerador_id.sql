{% macro gerador_id(coluna_id, nome_tabela) %}
    -- gerador dinâmico de IDs
    coalesce(
        (select max(cast({{ coluna_id }} as int)) from dbt_cloud.{{ nome_tabela }}), 0
    )
    + row_number() over ()

{% endmacro %}