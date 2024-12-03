{{
    config(
        materialized="view",
        name="categoria_agregada",
        description="Valor pago por categoria de estabelecimento nos últimos 6 meses",
    )
}}

WITH ultimos_seis_meses AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)) AS INT64) AS mes_limite
)

SELECT
    CONCAT(b.idmes, a.id_categoria) AS id,
    b.idmes AS mes_pagamento,
    a.id_categoria,
    COUNT(a.id_movimentacao) AS qtd_compras,
    SUM(a.valor_pago_alterado) AS valor_pago_categoria,
    SUM(a.valor_pago_alterado) / COUNT(a.id_movimentacao) AS media_categoria
FROM {{ source("dbt", "fato") }} a
LEFT JOIN {{ source("dbt", "tempo") }} b 
    ON a.data_pagamento = b.iddata
JOIN ultimos_seis_meses u 
    ON CAST(b.idmes AS INT64) >= u.mes_limite
GROUP BY 
    1, 2, 3
ORDER BY 
    mes_pagamento DESC, 
    valor_pago_categoria DESC