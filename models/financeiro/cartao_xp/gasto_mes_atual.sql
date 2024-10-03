{{
    config(
        materialized="view",
        name="gasto_mes_atual",
        description="Valor gasto da fatura mais recente",
    )
}}

select
    format_date('%Y%m', data_pagamento) as idMesPagamento,
    indicativo_compra_parcelada,
    cod_estabelecimento,
    sum(valor_pago_alterado) as valor_pago_alterado
from {{ source("dbt", "fato") }}
where
    cast(format_date('%Y%m', data_pagamento) as int64) = (
        select max(cast(format_date('%Y%m', data_pagamento) as int64))
        from {{ source("dbt", "fato") }}
    )
group by 1,2,3