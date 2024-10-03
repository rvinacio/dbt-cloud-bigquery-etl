{{
    config(
        materialized="table",
        name="tb_mes",
        description="tabela calendário por mês",
        unique_key="idMes"
    )
}}

select distinct idMes, ano_mes, descricaoMes, descricaoResumoMes, ano
from `rafael-data.dbt_cloud.tb_tempo`
order by idMes DESC