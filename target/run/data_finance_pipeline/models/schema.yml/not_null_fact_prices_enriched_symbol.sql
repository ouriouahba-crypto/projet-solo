
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "postgres"."public"."fact_prices_enriched"
where symbol is null



  
  
      
    ) dbt_internal_test