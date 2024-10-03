{{
    config(
        materialized="view",
        name="categoria_agregada_mes_atual",
        description="Valor pago por categoria de estabelecimento da fatura mais recente",
    )
}}

/*
Esta view agrega os valores pagos por categoria de estabelecimento para o mês de pagamento mais recente.
*/
select
    concat(b.idmes, a.id_categoria, a.cod_estabelecimento) as id,  -- ID único combinando mês e categoria
    b.idmes as mes_pagamento,  -- Mês de pagamento da fatura
    a.id_categoria,  -- ID da categoria do estabelecimento
    a.cod_estabelecimento,  -- ID unificado do estabelecimento
    count(a.id_movimentacao) as qtd_compras,  -- Quantidade total de compras na categoria
    sum(a.valor_pago_alterado) as valor_pago_categoria,  -- Valor total pago na categoria
    sum(a.valor_pago_alterado) / count(a.id_movimentacao) as media_categoria  -- Média de valor pago por compra na categoria

from {{ source("dbt", "fato") }} a  -- Tabela de fatos com informações das compras
left join {{ source("dbt", "tempo") }} b on (a.data_pagamento = b.iddata)  -- Junção com tabela de tempo para obter o mês de pagamento

/*
Filtra apenas as compras do mês de pagamento mais recente.
*/
where
    cast(format_date('%Y%m', a.data_pagamento) as int64) = (
        select max(cast(format_date('%Y%m', data_pagamento) as int64))
        from {{ source("dbt", "fato") }}
    )

group by 1, 2, 3, 4  -- Agrupa por todas as colunas selecionadas, exceto as métricas agregadas
order by mes_pagamento desc, valor_pago_categoria desc  -- Ordena pelo mês de pagamento (descendente) e valor pago (descendente)