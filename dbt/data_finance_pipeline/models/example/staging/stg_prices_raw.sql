{{ config(materialized='view') }}

select
    *
from public.raw_prices
