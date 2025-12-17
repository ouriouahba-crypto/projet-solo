
  create view "postgres"."public"."fact_prices_enriched__dbt_tmp"
    
    
  as (
    with base as (
    select
        symbol,
        dt,
        source,
        open,
        high,
        low,
        close,
        volume,
        load_ts
    from "postgres"."public"."fact_prices"
),

returns as (
    select
        symbol,
        dt,
        source,
        open,
        high,
        low,
        close,
        volume,
        load_ts,
        lag(close) over (partition by symbol order by dt) as prev_close
    from base
),

with_returns as (
    select
        symbol,
        dt,
        source,
        open,
        high,
        low,
        close,
        volume,
        load_ts,
        prev_close,
        case
            when prev_close is null then null
            else (close / prev_close) - 1
        end as daily_return
    from returns
),

with_sma as (
    select
        symbol,
        dt,
        source,
        open,
        high,
        low,
        close,
        volume,
        load_ts,
        daily_return,
        avg(close) over (
            partition by symbol
            order by dt
            rows between 19 preceding and current row
        ) as sma_20,
        avg(close) over (
            partition by symbol
            order by dt
            rows between 49 preceding and current row
        ) as sma_50
    from with_returns
)

select *
from with_sma
  );