{{
    config(
        materialized="view",
        name="comport_mensal_cat",
    )
}}

-- Consulta SQL para agregar dados de compras mensais por categoria
select
    upper(concat(b.descricaomes, a.id_categoria)) as id,  -- Gera um ID único concatenando mês e categoria (em maiúsculas)
    b.descricaomes as descricaomes,  -- Mês da compra
    a.id_categoria,  -- ID da categoria da compra
    count(a.cod_estabelecimento) as qtd_compras,  -- Quantidade total de compras na categoria e mês
    count(distinct b.ano) as qtd_ano,  -- Quantidade de anos em que ocorreram compras na categoria
    (sum(a.valor_pago_alterado) / count(distinct b.ano)) as valor_pago  -- Valor médio pago por ano na categoria e mês

from {{ source("dbt", "fato") }} a  -- Acessa a tabela "fato" do modelo "dbt" (provavelmente a tabela de fatos com os dados de compras)
left join {{ source("dbt", "tempo") }} b on (a.data_compra = b.iddata)  -- Junta com a tabela de dimensão de tempo para obter o mês da compra
left join {{ source("dbt", "cat") }} c on (a.id_categoria = c.id_categoria)  -- Junta com a tabela de dimensão de categoria para obter o nome da categoria

group by 1, 2, 3  -- Agrupa os resultados por id, mes e id_categoria

order by id  -- Ordena os resultados pelo ID gerado