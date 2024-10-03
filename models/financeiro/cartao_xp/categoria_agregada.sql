{{
    config(
        materialized="view",
        name="categoria_agregada",
        description="Valor pago por categoria de estabelecimento",
    )
}}

select
    concat(b.idmes, a.id_categoria) as id,
    b.idmes as mes_pagamento,
    a.id_categoria,
    count(a.id_movimentacao) as qtd_compras,
    sum(a.valor_pago_alterado) as valor_pago_categoria,
    sum(a.valor_pago_alterado) / count(a.id_movimentacao) as media_categoria
from {{ source("dbt", "fato") }} a
left join {{ source("dbt", "tempo") }} b on (a.data_pagamento = b.iddata)
group by 1, 2, 3
order by mes_pagamento desc, valor_pago_categoria desc