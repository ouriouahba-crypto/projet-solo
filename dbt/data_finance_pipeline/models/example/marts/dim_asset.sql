{{ config(materialized='table') }}

select
  row_number() over (order by symbol) as asset_id,
  symbol
from (
  select distinct symbol
  from public.raw_prices
  where symbol is not null
) s
