{{ config(materialized='table') }}

with base_dates as (

    select distinct
        date::date as date
    from {{ ref('stg_prices_raw') }}

)

select
    date,
    extract(year from date)  as year,
    extract(month from date) as month,
    extract(day from date)   as day,
    extract(dow from date)   as day_of_week,
    extract(week from date)  as week
from base_dates
