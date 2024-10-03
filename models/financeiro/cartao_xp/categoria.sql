{{
    config(
        materialized="table",
        name="categoria",
        description="Lista as categorias",
        unique_key="id_categoria",
    )
}}

select
    {{ gerador_id("id_categoria", "categoria") }} as id_categoria,
    categoria,
    tags,
    'rvinacio.dev@gmail.com' as usuario,
    current_timestamp() as dt_inclusao
from dbt_cloud.tags_categoria