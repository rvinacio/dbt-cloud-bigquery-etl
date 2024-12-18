{{
    config(
        materialized="incremental",
        name="estabelecimento_auxiliar",
        description="Lista novos estabelecimentos para carga",
        unique_key="estabelecimento",
    )
}}


select
    farm_fingerprint(UPPER(f.estabelecimento)) as cod_estabelecimento,
    upper(cast(f.estabelecimento as string)) as estabelecimento,
    min(CAST(REGEXP_EXTRACT(_file, r'\d{4}-\d{2}-\d{2}') as date)) AS dt_primeiro_arquivo,
    max(f._fivetran_synced) as _fivetran_synced

from {{ source("import", "fatura") }} f

where
    upper(f.estabelecimento) <> "PAGAMENTOS VALIDOS NORMAIS"
    and farm_fingerprint(UPPER(f.estabelecimento)) not in (
        select distinct cod_estabelecimento
        from {{ source("dbt", "estab_aux") }}
    )

group by 1, 2
order by estabelecimento