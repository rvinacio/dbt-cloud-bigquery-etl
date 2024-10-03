{{
    config(
        materialized="incremental",
        name="alteracao_valor_pago",
        description="Esta tabela serve para alerar valor pago quando o pagamento for de terceiros",
        unique_key="_key",
    )
}}

-- Este script SQL no dbt Cloud realiza ajustes no valor pago de transações de cartão de crédito, 
-- considerando regras de alteração e informações sobre parcelamento.

SELECT
    a._key,                                                -- Chave única da transação
    CAST(FORMAT_DATE('%Y%m', a.data_pagamento) AS int64) AS idmespagamento,  -- ID do mês de pagamento (AAAAMM)
    a.data_pagamento,                                       -- Data do pagamento
    a.id_movimentacao,                                    -- ID da movimentação
    a.cod_estabelecimento,                                 -- Código do estabelecimento
    a.estabelecimento,                                     -- Nome do estabelecimento
    a.id_categoria,                                        -- ID da categoria
    "" AS nome_produto,                                     -- Nome do produto (vazio neste caso)
    a.data_compra,                                         -- Data da compra
    a.valor_pago,                                          -- Valor original pago

    -- Ajusta o valor pago com base nas regras de alteração
    CASE
        WHEN b.id_regra_alteracao_valor = '2A' THEN 0       -- Regra 2A: zera o valor
        WHEN b.id_regra_alteracao_valor = '5A' THEN (a.valor_pago / 2)  -- Regra 5A: divide o valor por 2
        ELSE a.valor_pago_alterado                          -- Mantém o valor alterado se existir, caso contrário, o valor original
    END AS valor_pago_alterado,

    -- Identifica a regra de alteração aplicada
    CASE
        WHEN b.id_regra_alteracao_valor IN ('2A', '5A') THEN b.id_regra_alteracao_valor
        ELSE '0A'                                           -- Regra 0A: nenhuma alteração
    END AS id_regra_alteracao_valor,

    a.indicativo_compra_parcelada,                         -- Indica se a compra é parcelada (s/n)

    -- Indica se houve alteração no valor
    CASE
        WHEN b.id_regra_alteracao_valor IN ('2A', '5A') THEN 's'
        ELSE a.indicativo_alteracao_valor                   -- Mantém o indicador de alteração se existir, caso contrário, 'n'
    END AS indicativo_alteracao_valor,

    a.indicativo_parcela_a_vencer,                         -- Indica se há parcelas a vencer (s/n)
    a._fivetran_synced,                                    -- Data de sincronização Fivetran
    CONCAT("CARGA DBT/MODELO: alteracao_valor_pago: ", current_timestamp()) AS motivo_alteracao,  -- Motivo da alteração (carga do modelo)
    COALESCE(a.data_alteracao, a._fivetran_synced) AS data_alteracao,  -- Data da alteração (se existir, senão data de sincronização)
    a.contador,                                            -- Contador para identificar duplicatas
    a.id_compra_parcelada                                  -- ID da compra parcelada

FROM {{ source("dbt", "fato") }} AS a                     -- Lê dados da tabela de fatos
LEFT JOIN `rafael-data.dbt_cloud.historico_ajuste_parcelas` AS b  -- Junta com histórico de ajustes de parcelas
    ON a.id_compra_parcelada = b.id_compra_parcelada        -- Usando o ID da compra parcelada como chave de junção

WHERE a._fivetran_synced > (                             -- Filtra transações mais recentes que a última carga na tabela "valor_alterado"
    SELECT COALESCE(MAX(_fivetran_synced), CAST('2020-01-01' AS timestamp))
    FROM {{ source("dbt", "valor_alterado") }}
)