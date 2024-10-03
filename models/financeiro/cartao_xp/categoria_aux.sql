{{
    config(
        materialized="table",
    )
}}

-- backup da tabela categoria

select * from {{source("dbt","cat")}}
order by id_categoria