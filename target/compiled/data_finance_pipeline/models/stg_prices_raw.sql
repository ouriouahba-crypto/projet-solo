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