
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT * 
FROM `messud`.`fact_observations`
where Occurrence < 0
  
  
    ) dbt_internal_test