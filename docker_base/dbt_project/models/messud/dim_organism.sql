{{ config(
    materialized='incremental',
    unique_key='Name',
    incremental_strategy='delete+insert'
) }}

WITH gbif_organisms AS (
    SELECT
        {{ get_organism_type("species") }} AS Type,
        species AS Name,
        scientificName AS ScientificName,
        {{ get_unit("species") }} AS Unit
    FROM {{ ref('stg_gbif') }}
),

aphis_organisms AS (
    SELECT
        {{ get_organism_type("pest_name") }} AS Type,
        pest_name AS Name,
        'NA' AS ScientificName,
        {{ get_unit("pest_name") }} AS Unit
    FROM {{ ref('stg_aphis_long') }}
),

organismsCombined AS (
    SELECT Type, Name, ScientificName, Unit FROM gbif_organisms
    UNION DISTINCT
    SELECT Type, Name, ScientificName, Unit FROM aphis_organisms
),

current_dim AS (
    SELECT *
    FROM {{ this }}
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
    {{ dbt_utils.generate_surrogate_key(['Name', 'now()']) }} AS OrganismKey,
    s.Type, s.Name, s.ScientificName, s.Unit,
    now() AS valid_from,
    NULL AS valid_to,
    1 AS is_current
FROM organismsCombined s
WHERE s.Name IN (SELECT Name FROM changes)

{% if is_incremental() %}
UNION ALL

-- expire existing changed
SELECT
    d.OrganismKey,
    d.Type, d.Name, d.ScientificName, d.Unit,
    d.valid_from,
    now() AS valid_to,
    0 AS is_current
FROM {{ this }} d
WHERE d.Name IN (SELECT Name FROM changes)
  AND d.is_current = 1
{% endif %}
