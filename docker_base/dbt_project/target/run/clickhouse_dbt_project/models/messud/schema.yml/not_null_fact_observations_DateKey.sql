
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DateKey
from `messud`.`fact_observations`
where DateKey is null



  
  
    ) dbt_internal_test