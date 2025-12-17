
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "postgres"."public"."prices_raw"
where symbol is null



  
  
      
    ) dbt_internal_test