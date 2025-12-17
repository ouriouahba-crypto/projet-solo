
  create view "postgres"."public"."stg_prices_raw__dbt_tmp"
    
    
  as (
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
from "postgres"."public"."prices_raw"
  );