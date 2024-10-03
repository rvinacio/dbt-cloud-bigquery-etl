{{
    config(
        materialized="view",
        name="agregada_mes",
        description="Fatura xp por mês",
    )
}}

select
    b.idmes,
    a.cod_estabelecimento,
    a.id_categoria,
    sum(a.valor_pago_alterado) as valor_pago_alterado,
from {{ source("dbt", "fato") }} a
left join {{ source("dbt", "tempo") }} b on (a.data_pagamento = b.iddata)
group by 1, 2, 3
order by b.idmes desc