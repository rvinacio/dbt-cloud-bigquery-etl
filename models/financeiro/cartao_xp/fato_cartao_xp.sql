{{
    config(
        materialized="incremental",
        name="fato_cartao_xp",
        description="Trata e registra os dados novos e atualiza dados já gravados da fatura de cartão de crédito XP",
        unique_key="_key",
    )
}}

-- Este script SQL processa dados de faturas de cartão de crédito, aplicando transformações e ajustes para gerar um modelo de dados consolidado.

WITH 

    -- Encontra a data de sincronização mais recente na tabela de fatos
    maxdatasynced AS (
        SELECT COALESCE(MAX(_fivetran_synced), CAST('2020-01-01' AS timestamp)) AS max_fivetran_synced
        FROM {{ source("dbt", "fato") }}  -- Referência à tabela de fatos
    ),

    -- Realiza o tratamento inicial dos dados da tabela "fatura"
    tratando_dados AS (
        SELECT 
            CAST(REGEXP_EXTRACT(_file, r'\d{4}-\d{2}-\d{2}') as date) AS data_pagamento,  -- Extrai a data de pagamento do nome do arquivo
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(f.valor, r'R\$', '')), r'\.', ''), r',', '.') AS float64) AS valor_pago,  -- Limpa e converte o valor pago
            CAST((CASE WHEN f.parcela = '-' THEN CAST(0 AS string) ELSE LTRIM(LEFT(f.parcela, 2)) END) AS int64) AS parcela_vigente,  -- Extrai a parcela vigente
            CAST((CASE WHEN f.parcela = '-' THEN CAST(0 AS string) ELSE RTRIM(RIGHT(f.parcela, 2)) END) AS int64) AS parcela_total,  -- Extrai a parcela total
            SAFE_CAST(REGEXP_REPLACE(f.data, r'(\d{2})/(\d{2})/(\d{4})', '\\3-\\2-\\1') AS date) AS data_compra,  -- Converte a data da compra
            e.estabelecimento,  -- Nome do estabelecimento
            e.cod_estabelecimento,  -- Código do estabelecimento
            e.id_categoria,  -- ID da categoria do estabelecimento
            c.categoria,  -- Nome da categoria do estabelecimento
            CONCAT(f._file, f._line) AS id_movimentacao,  -- ID único para cada movimentação
            f._fivetran_synced  -- Data de sincronização da movimentação
        FROM {{ source("import", "fatura") }} f  -- Referência à tabela de faturas
        LEFT JOIN {{ source("dbt", "estab") }} e ON farm_fingerprint(UPPER(f.estabelecimento)) = e.cod_estabelecimento  -- Junção com a tabela de estabelecimentos
        LEFT JOIN {{ source("dbt", "cat") }} c ON e.id_categoria = c.id_categoria  -- Junção com a tabela de categorias
        WHERE UPPER(f.estabelecimento) NOT IN ('PAGAMENTOS VALIDOS NORMAIS', 'PAGAMENTO DE FATURA')  -- Filtra estabelecimentos específicos
          AND f._fivetran_synced > (SELECT max_fivetran_synced FROM maxdatasynced)  -- Filtra por data de sincronização
    ),

    -- Adiciona informações sobre parcelas e identifica linhas duplicadas
    tratando_dados_2 AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(farm_fingerprint(CONCAT(data_pagamento, cod_estabelecimento, data_compra, valor_pago, _fivetran_synced)) AS string)
                ORDER BY id_movimentacao
            ) AS contador,  -- Contador para identificar duplicatas
            CAST(farm_fingerprint(CONCAT(cod_estabelecimento, data_compra, parcela_total)) AS string) AS id_compra_parcelada,  -- ID para compras parceladas
            CASE WHEN parcela_total > 0 THEN 's' ELSE 'n' END AS indicativo_compra_parcelada,  -- Indica se a compra é parcelada
            CASE WHEN (parcela_total - parcela_vigente) > 0 THEN 's' ELSE 'n' END AS indicativo_parcela_a_vencer,  -- Indica se há parcelas a vencer
            CAST((parcela_total - parcela_vigente) AS int64) AS parcela_a_vencer  -- Calcula o número de parcelas a vencer
        FROM tratando_dados
    ),

    -- Obtém histórico de ajustes de parcelas
    historico_ajuste_parcelas AS (
        SELECT a.*
        FROM `rafael-data.dbt_cloud.historico_ajuste_parcelas` a
        INNER JOIN tratando_dados_2 b ON a.id_compra_parcelada = b.id_compra_parcelada
        WHERE id_ativado = true  -- Filtra apenas ajustes ativados
    ),

    -- Trata a coluna _key, gerando um hash único para cada linha
    tratando_key AS ( 
        SELECT
            a.*,
            CAST(farm_fingerprint(CONCAT(
                a.data_pagamento,
                a.cod_estabelecimento,
                a.data_compra,
                a.valor_pago,
                a._fivetran_synced,
                a.contador
            )) AS string) AS _key
        FROM tratando_dados_2 a
    )

-- Consulta final que combina os dados tratados e aplica ajustes de valor
SELECT
    a.id_movimentacao,
    a.data_pagamento,
    a.cod_estabelecimento,
    a.estabelecimento,
    a.id_categoria,
    a.categoria,
    a.data_compra,
    a.indicativo_compra_parcelada,
    a.indicativo_parcela_a_vencer,
    a.parcela_vigente,
    a.parcela_total,
    a.parcela_a_vencer,
    a.valor_pago,
    a._fivetran_synced,
    CASE
        WHEN (b.id_regra_alteracao_valor = '2A' AND b.id_ativado = TRUE) THEN 0  -- Regra de ajuste 2A
        WHEN (b.id_regra_alteracao_valor = '5A' AND b.id_ativado = TRUE) THEN (a.valor_pago / 2)  -- Regra de ajuste 5A
        WHEN (c.id_regra_alteracao_valor = '1A' AND c.id_ativado = TRUE) THEN 0
        WHEN (c.id_regra_alteracao_valor IN ('3A','4A') AND c.id_ativado = TRUE) THEN c.valor_pago_alterado
        ELSE a.valor_pago 
    END AS valor_pago_alterado,
    CASE
        WHEN (a.id_compra_parcelada = b.id_compra_parcelada AND b.id_ativado = TRUE) THEN 's'  -- Indica se houve alteração
        WHEN (a._key = c._key AND c.id_ativado = TRUE) THEN 's'  -- Indica se houve alteração
        ELSE 'n'
    END AS indicativo_alteracao_valor,
    current_timestamp() AS data_alteracao,  -- Data da alteração
    CONCAT("DBT CLOUD/MODELO: fato_cartao_xp: ", current_timestamp()) AS motivo_alteracao,  -- Motivo da alteração
    a._key,  -- Chave única da linha
    CAST(a.contador as STRING) as contador,
    "" AS nome_produto,  -- Campo vazio para nome do produto
    a.id_compra_parcelada,  -- ID da compra parcelada
    CASE
        WHEN (a.id_compra_parcelada = b.id_compra_parcelada AND b.id_ativado = TRUE) THEN b.id_regra_alteracao_valor
        WHEN (a._key = c._key AND c.id_ativado = TRUE) THEN c.id_regra_alteracao_valor
        ELSE '0A'
    END  AS id_regra_alteracao_valor

FROM tratando_key as a  -- Usa a CTE "tratando_key"
LEFT JOIN historico_ajuste_parcelas as b ON a.id_compra_parcelada = b.id_compra_parcelada  -- Junção com o histórico de ajustes
LEFT JOIN `dbt_cloud.historico_ajuste_a_vista` as c ON a._key = c._key