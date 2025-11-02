
  
    
    
    
        
         


        insert into `messud`.`dim_organism__dbt_backup`
        ("OrganismKey", "Type", "Name", "ScientificName", "Unit")WITH gbif_organisms AS (
    SELECT
        
    CASE
        WHEN species = 'Apis mellifera' THEN 'bee'
        WHEN species IN ('Varroa', 'Nosema') THEN 'pest'
        WHEN species IN ('abpv','amsv1','cbpv','dwv','dwv-b','iapv','kbv','lsv2','sbpv','mkv') THEN 'virus'
        ELSE 'Unknown'
    END
 AS Type,
        species AS Name,
        scientificName AS ScientificName,
        
CASE
    WHEN species = 'Apis mellifera' THEN 'individuals'
    WHEN species = 'Varroa' THEN 'individuals per 100 bees'
    WHEN species = 'Nosema' THEN 'million spores per bee'
    WHEN species IN ('abpv','amsv1','cbpv','dwv','dwv-b','iapv','kbv','lsv2','sbpv','mkv')
        THEN '% of infected bees'
    ELSE 'Unknown'
END
 AS Unit
    FROM `messud`.`stg_gbif`
),

aphis_organisms AS (
    SELECT
        
    CASE
        WHEN pest_name = 'Apis mellifera' THEN 'bee'
        WHEN pest_name IN ('Varroa', 'Nosema') THEN 'pest'
        WHEN pest_name IN ('abpv','amsv1','cbpv','dwv','dwv-b','iapv','kbv','lsv2','sbpv','mkv') THEN 'virus'
        ELSE 'Unknown'
    END
 AS Type,
        pest_name AS Name,
        'NA' AS ScientificName,
        
CASE
    WHEN pest_name = 'Apis mellifera' THEN 'individuals'
    WHEN pest_name = 'Varroa' THEN 'individuals per 100 bees'
    WHEN pest_name = 'Nosema' THEN 'million spores per bee'
    WHEN pest_name IN ('abpv','amsv1','cbpv','dwv','dwv-b','iapv','kbv','lsv2','sbpv','mkv')
        THEN '% of infected bees'
    ELSE 'Unknown'
END
 AS Unit
    FROM `messud`.`stg_aphis_long`
),

organismsCombined AS (
    SELECT Type, Name, ScientificName, Unit FROM gbif_organisms
    UNION ALL
    SELECT Type, Name, ScientificName, Unit FROM aphis_organisms
)

SELECT
    lower(hex(MD5(toString(coalesce(cast(Name as String), '_dbt_utils_surrogate_key_null_') )))) AS OrganismKey,
    Type,
    Name,
    ScientificName,
    Unit
FROM organismsCombined

ORDER BY Type, Name, ScientificName
  