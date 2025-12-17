select
    symbol,
    source
from "postgres"."public"."prices_raw"
group by
    symbol,
    source