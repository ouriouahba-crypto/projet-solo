
  create view "postgres"."public"."dim_asset__dbt_tmp"
    
    
  as (
    select
    symbol,
    source
from "postgres"."public"."prices_raw"
group by
    symbol,
    source
  );