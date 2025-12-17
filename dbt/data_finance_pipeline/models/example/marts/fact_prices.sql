{{ config(materialized='table') }}

with prices as (

    select
        date::date as date,
        symbol,
        open,
        high,
        low,
        close
    from {{ ref('stg_prices_raw') }}

)

select
    p.date,
    a.asset_id,
    p.symbol,
    p.open,
    p.high,
    p.low,
    p.close
from prices p
left join {{ ref('dim_calendar') }} c
    on c.date = p.date
left join {{ ref('dim_asset') }} a
    on a.symbol = p.symbol
