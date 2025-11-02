
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select OrganismKey
from `messud`.`fact_observations`
where OrganismKey is null



  
  
    ) dbt_internal_test