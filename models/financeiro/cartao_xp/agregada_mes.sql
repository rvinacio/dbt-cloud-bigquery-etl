{{
    config(
        materialized="view",
        name="agregada_mes",
        description="Fatura XP por mês",
    )
}}

WITH ultimos_seis_meses AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)) AS INT64) AS mes_limite
)

SELECT
    b.idmes,
    a.cod_estabelecimento,
    a.id_categoria,
    SUM(a.valor_pago_alterado) AS valor_pago_alterado
FROM {{ source("dbt", "fato") }} a
LEFT JOIN {{ source("dbt", "tempo") }} b 
    ON a.data_pagamento = b.iddata
JOIN ultimos_seis_meses u 
    ON CAST(b.idmes AS INT64) >= u.mes_limite
GROUP BY 
    1, 2, 3
ORDER BY 
    b.idmes DESC