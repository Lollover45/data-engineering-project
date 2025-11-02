SELECT * 
FROM {{ref ('fact_observations')}}
where Occurrence < 0