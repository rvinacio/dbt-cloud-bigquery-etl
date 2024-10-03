{{
    config(
        materialized="view",
        name="compras_parceladas",
        description="Extrai da fato principal as compras parceladas",
    )
}}


select
    concat(a.data_pagamento, a.cod_estabelecimento, a.id_categoria) as id,
    a.data_pagamento,
    b.idmes as mes_pagamento,
    a.id_categoria,
    a.data_compra,
    a.cod_estabelecimento,
    a.parcela_vigente,
    a.parcela_total,
    a.parcela_a_vencer,
    a.indicativo_parcela_a_vencer,
    date_add(a.data_pagamento, interval(a.parcela_a_vencer) month) data_ult_pgto,
    a.valor_pago_alterado as valor_parcela,
    (a.parcela_total - a.parcela_vigente) * (a.valor_pago_alterado) as valor_a_pagar,
    (a.parcela_total * a.valor_pago_alterado) as valor_total_compra

from {{ source("dbt", "fato") }} a
left join {{ source("dbt", "tempo") }} b on (a.data_pagamento = b.iddata)

where
    a.indicativo_parcela_a_vencer = 's'
    and a.data_pagamento = (select max(data_pagamento) from {{ source("dbt", "fato") }})

order by a.data_compra