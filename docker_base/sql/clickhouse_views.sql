-- docker_base/sql/clickhouse_views.sql
-- Analytical views over gold schema with full vs pseudonymized access

CREATE DATABASE IF NOT EXISTS messud;
USE messud;

-- FULL analytical view
CREATE OR REPLACE VIEW v_bee_observations_full AS
SELECT
    -- Fact
    fo.OrganismKey,
    fo.LocationKey,
    fo.DateKey,
    fo.Occurrence,

    -- Date dimension
    dd.FullDate      AS observation_date,
    dd.Year,
    dd.Month,
    dd.Season,
    dd.Decade,

    -- Location dimension  (these will be pseudonymized in limited view)
    dl.Continent,
    dl.Country,
    dl.State,
    dl.County,

    -- Organism dimension
    do.Type          AS OrganismType,
    do.Name          AS OrganismName,
    do.ScientificName,
    do.Unit
FROM fact_observations AS fo
JOIN dim_date      AS dd ON fo.DateKey      = dd.DateKey
JOIN dim_location  AS dl ON fo.LocationKey  = dl.LocationKey
JOIN dim_organism  AS do ON fo.OrganismKey  = do.OrganismKey;


-- LIMITED analytical view
-- Same schema, but 3 “sensitive” columns (Country, State, County) are pseudonymized
CREATE OR REPLACE VIEW v_bee_observations_limited AS
SELECT
    fo.OrganismKey,
    fo.LocationKey,
    fo.DateKey,
    fo.Occurrence,

    dd.FullDate      AS observation_date,
    dd.Year,
    dd.Month,
    dd.Season,
    dd.Decade,

    dl.Continent,
    cityHash64(dl.Country) AS Country,
    cityHash64(dl.State)   AS State,
    cityHash64(dl.County)  AS County,

    do.Type          AS OrganismType,
    do.Name          AS OrganismName,
    do.ScientificName,
    do.Unit
FROM fact_observations AS fo
JOIN dim_date      AS dd ON fo.DateKey      = dd.DateKey
JOIN dim_location  AS dl ON fo.LocationKey  = dl.LocationKey
JOIN dim_organism  AS do ON fo.OrganismKey  = do.OrganismKey;
