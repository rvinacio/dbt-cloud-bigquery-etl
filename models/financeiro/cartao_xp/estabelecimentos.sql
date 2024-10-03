{{
    config(
        materialized="incremental",
        name="estabelecimentos",
        description="Tabela de estabelecimentos do cartão do cartão XP",
        unique_key="cod_estabelecimento",
    )
}}

select
    a.cod_estabelecimento,
    a.estabelecimento,
    2 as id_cartao,
    a.dt_primeiro_arquivo,
    {{ categorizar("a.estabelecimento") }} as id_categoria,
    false as favoritar,
    a._fivetran_synced,
    current_timestamp() as data_alteracao,
    concat(
        "DBT CLOUD/MODELO: estabelecimentos: ", current_timestamp()
    ) as origem_alteracao

from {{ source("dbt", "estab_aux") }} a
left join
    {{ source("dbt", "estab") }} b on (a.cod_estabelecimento = b.cod_estabelecimento)

where b.cod_estabelecimento is null