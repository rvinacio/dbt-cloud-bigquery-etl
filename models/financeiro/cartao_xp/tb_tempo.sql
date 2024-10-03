{{
    config(
        materialized="table",
        name="tb_tempo",
        description="tabela calendário",
        unique_key="idData",
    )
}}

-- Define source data with GENERATE_DATE_ARRAY
with
    all_dates as (
        select
            generate_date_array(
                date '2020-01-01', date '2030-12-31', interval 1 day
            ) as date_array
    )

-- Main select statement
select
    cast(format_date('%Y-%m-%d', date) as date) as idData,
    format_date('%Y%m', date) as idMes,
    case
        when extract(month from date) = 1
        then 'Janeiro'
        when extract(month from date) = 2
        then 'Fevereiro'
        when extract(month from date) = 3
        then 'Março'
        when extract(month from date) = 4
        then 'Abril'
        when extract(month from date) = 5
        then 'Maio'
        when extract(month from date) = 6
        then 'Junho'
        when extract(month from date) = 7
        then 'Julho'
        when extract(month from date) = 8
        then 'Agosto'
        when extract(month from date) = 9
        then 'Setembro'
        when extract(month from date) = 10
        then 'Outubro'
        when extract(month from date) = 11
        then 'Novembro'
        when extract(month from date) = 12
        then 'Dezembro'
        else null
    end as descricaoMes,
    concat(
        case
            when extract(month from date) = 1
            then 'Jan'
            when extract(month from date) = 2
            then 'Fev'
            when extract(month from date) = 3
            then 'Mar'
            when extract(month from date) = 4
            then 'Abr'
            when extract(month from date) = 5
            then 'Mai'
            when extract(month from date) = 6
            then 'Jun'
            when extract(month from date) = 7
            then 'Jul'
            when extract(month from date) = 8
            then 'Ago'
            when extract(month from date) = 9
            then 'Set'
            when extract(month from date) = 10
            then 'Out'
            when extract(month from date) = 11
            then 'Nov'
            when extract(month from date) = 12
            then 'Dez'
            else null
        end,
        '/',
        format_date('%Y', date)
    ) as descricaoResumoMes,
    concat(
        case
            when extract(dayofweek from date) = 1
            then 'Domingo'
            when extract(dayofweek from date) = 2
            then 'Segunda-feira'
            when extract(dayofweek from date) = 3
            then 'Terça-feira'
            when extract(dayofweek from date) = 4
            then 'Quarta-feira'
            when extract(dayofweek from date) = 5
            then 'Quinta-feira'
            when extract(dayofweek from date) = 6
            then 'Sexta-feira'
            when extract(dayofweek from date) = 7
            then 'Sábado'
            else null
        end,
        ', ',
        format_date('%d de ', date),
        case
            when extract(month from date) = 1
            then 'Janeiro'
            when extract(month from date) = 2
            then 'Fevereiro'
            when extract(month from date) = 3
            then 'Março'
            when extract(month from date) = 4
            then 'Abril'
            when extract(month from date) = 5
            then 'Maio'
            when extract(month from date) = 6
            then 'Junho'
            when extract(month from date) = 7
            then 'Julho'
            when extract(month from date) = 8
            then 'Agosto'
            when extract(month from date) = 9
            then 'Setembro'
            when extract(month from date) = 10
            then 'Outubro'
            when extract(month from date) = 11
            then 'Novembro'
            when extract(month from date) = 12
            then 'Dezembro'
            else null
        end,
        ' de ',
        format_date('%Y', date)
    ) as descricaoData,
    format_date('%Y', date) as ano,
    concat(format_date('%Y', date), '/', format_date('%m', date)) as ano_mes
from all_dates, unnest(date_array) as date
order by date