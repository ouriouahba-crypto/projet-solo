
  create view "postgres"."public"."dim_calendar__dbt_tmp"
    
    
  as (
    with bounds as (
    select
        min(dt)::date as min_dt,
        max(dt)::date as max_dt
    from "postgres"."public"."prices_raw"
),
dates as (
    select
        generate_series(min_dt, max_dt, interval '1 day')::date as dt
    from bounds
)

select dt
from dates
  );