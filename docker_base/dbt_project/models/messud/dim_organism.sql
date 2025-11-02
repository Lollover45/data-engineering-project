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
    UNION ALL
    SELECT Type, Name, ScientificName, Unit FROM aphis_organisms
)

SELECT
    {{dbt_utils.generate_surrogate_key(['Name']) }} AS OrganismKey,
    Type,
    Name,
    ScientificName,
    Unit
FROM organismsCombined

ORDER BY Type, Name, ScientificName
