
        
  
    
    
    
        
         


        insert into `messud`.`dim_organism__dbt_new_data_9ff22b18_09a1_4f40_820e_cd479ff074e6`
        ("OrganismKey", "Type", "Name", "ScientificName", "Unit", "valid_from", "valid_to", "is_current")

WITH gbif_organisms AS (
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
    UNION DISTINCT
    SELECT Type, Name, ScientificName, Unit FROM aphis_organisms
),

current_dim AS (
    SELECT *
    FROM `messud`.`dim_organism`
    WHERE is_current = 1
),

changes AS (
    SELECT s.*
    FROM organismsCombined s
    LEFT JOIN current_dim d ON s.Name = d.Name
    WHERE d.Name IS NULL
       OR s.Unit != d.Unit
       OR s.Type != d.Type
       OR s.ScientificName != d.ScientificName
)

-- ✅ Final output: inserts only
SELECT
    lower(hex(MD5(toString(coalesce(cast(Name as String), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(now() as String), '_dbt_utils_surrogate_key_null_') )))) AS OrganismKey,
    s.Type, s.Name, s.ScientificName, s.Unit,
    now() AS valid_from,
    NULL AS valid_to,
    1 AS is_current
FROM organismsCombined s
WHERE s.Name IN (SELECT Name FROM changes)


UNION ALL

-- expire existing changed
SELECT
    d.OrganismKey,
    d.Type, d.Name, d.ScientificName, d.Unit,
    d.valid_from,
    now() AS valid_to,
    0 AS is_current
FROM `messud`.`dim_organism` d
WHERE d.Name IN (SELECT Name FROM changes)
  AND d.is_current = 1

  
      