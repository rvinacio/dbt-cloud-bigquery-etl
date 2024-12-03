{{
    config(
        materialized="view",
        name="view_fatura_xp",
        description="Resumo das faturas XP filtradas pelos últimos 6 meses"
    )
}}

WITH ultimos_seis_meses AS (
    SELECT
        DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS data_limite
)

SELECT
    id_movimentacao,
    data_pagamento,
    data_compra,
    estabelecimento,
    indicativo_compra_parcelada,
    parcela_total,
    parcela_vigente,
    SUM(valor_pago) AS valor_pago
FROM {{ source("dbt", "fato") }}
JOIN ultimos_seis_meses u 
    ON data_pagamento >= u.data_limite
GROUP BY 
    id_movimentacao, 
    data_pagamento, 
    data_compra, 
    estabelecimento, 
    indicativo_compra_parcelada, 
    parcela_total, 
    parcela_vigente