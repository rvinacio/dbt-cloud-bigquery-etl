{{
    config(
        materialized="view",
        name="view_fatura_xp"
    )
}}
select
    id_movimentacao,
    data_pagamento,
    data_compra,
    estabelecimento,
    indicativo_compra_parcelada,
    parcela_total,
    parcela_vigente,
    sum(valor_pago) as valor_pago
from {{ source("dbt", "fato") }}
group by 1, 2, 3, 4, 5, 6, 7