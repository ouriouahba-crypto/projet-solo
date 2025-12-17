
  create view "postgres"."public"."fact_prices__dbt_tmp"
    
    
  as (
    select
    pr.symbol,
    pr.dt,
    pr.source,
    pr.open,
    pr.high,
    pr.low,
    pr.close,
    pr.volume,
    pr.load_ts
from "postgres"."public"."prices_raw" pr
  );