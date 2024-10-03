{% macro gerador_id_estab_unificado() %}
    -- gerador de ID para estabelecimento unificado
    coalesce(
        (
            select max(cast(id_estab_unificado as int))
            from dbt_cloud.estabelecimento_unificado
        ),
        0
    )
    + row_number() over ()

{% endmacro %}