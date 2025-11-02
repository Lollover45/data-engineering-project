
  
    
    
    
        
         


        insert into `messud`.`stg_aphis_long__dbt_backup`
        ("sample_year", "sample_month_number", "sample_month", "state_code", "sampling_county", "pest_name", "value")SELECT
    sample_year,
    sample_month_number,
    sample_month,
    state_code,
    sampling_county,
    tupleElement(pest, 1) AS pest_name,
    tupleElement(pest, 2) AS value
FROM messud.aphis
ARRAY JOIN [
    tuple('Varroa', varroa_per_100_bees),
    tuple('Nosema', million_spores_per_bee),
    tuple('abpv', abpv_percentile),
    tuple('amsv1', amsv1_percentile),
    tuple('cbpv', cbpv_percentile),
    tuple('dwv', dwv_percentile),
    tuple('dwv-b', dwv_b_percentile),
    tuple('iapv', iapv_percentile),
    tuple('kbv', kbv_percentile),
    tuple('lsv2', lsv2_percentile),
    tuple('sbpv', sbpv_percentile),
    tuple('mkv', mkv_percentile)
] AS pest
  