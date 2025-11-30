--Question about viruses:
--From the three counties with the highest virus prevalence - how many bees were detected during the year?
WITH County AS (
  SELECT
    loc.County,
    avg(obs.Occurrence) FILTER (WHERE org.Type = 'virus') AS virus_total,
    SUM(obs.Occurrence) FILTER (WHERE org.Type = 'bee')   AS bee_total
  FROM messud.fact_observations obs
  JOIN messud.dim_organism  org ON org.OrganismKey = obs.OrganismKey
  JOIN messud.dim_location loc ON loc.LocationKey = obs.LocationKey
  JOIN messud.dim_date      dt ON dt.DateKey = obs.DateKey
  WHERE dt.Year = 2024
  GROUP BY loc.County
)
SELECT
  County,
  virus_total AS virus_count,
  bee_total   AS bee_count
FROM County
where bee_count is not null
ORDER BY virus_total DESC
LIMIT 3;



--Question about mites:
--How many bee occurrences are there in the 5 counties with the fewest Varroa mites?
WITH low_varroa AS (
    SELECT
        loc.County,
        AVG(obs.Occurrence) AS avg_varroa
    FROM messud.fact_observations obs
    JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
    JOIN messud.dim_location loc ON loc.LocationKey = obs.LocationKey
    WHERE org.Type = 'pest'
      AND org.Name = 'Varroa'
    GROUP BY loc.County
    ORDER BY avg_varroa ASC
)
SELECT 
  l.County AS county,
  l.avg_varroa AS average_varroa,
  SUM(obs.Occurrence) AS bee_occurrences
FROM low_varroa l
JOIN messud.dim_location loc ON loc.County = l.County
JOIN messud.fact_observations obs ON obs.LocationKey = loc.LocationKey
JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
WHERE org.Type = 'bee'
GROUP BY l.County, l.avg_varroa
ORDER BY l.avg_varroa ASC
limit 5;

-- How many bee occurrences are there in the 5 counties with the most Varroa mites?

WITH low_varroa AS (
    SELECT
        loc.County,
        AVG(obs.Occurrence) AS avg_varroa
    FROM messud.fact_observations obs
    JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
    JOIN messud.dim_location loc ON loc.LocationKey = obs.LocationKey
    WHERE org.Type = 'pest'
      AND org.Name = 'Varroa'
    GROUP BY loc.County
    ORDER BY avg_varroa desc
)
SELECT 
  l.County AS county,
  l.avg_varroa AS average_varroa,
  SUM(obs.Occurrence) AS bee_occurrences
FROM low_varroa l
JOIN messud.dim_location loc ON loc.County = l.County
JOIN messud.fact_observations obs ON obs.LocationKey = loc.LocationKey
JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
WHERE org.Type = 'bee'
GROUP BY l.County, l.avg_varroa
ORDER BY l.avg_varroa desc
limit 5;

-- Question about fungus:
--How many bee occurrences are there in the 5 counties with the fewest Nosema fungus?
WITH low_nosema AS (
    SELECT
        loc.County,
        AVG(obs.Occurrence) AS avg_nosema
    FROM messud.fact_observations obs
    JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
    JOIN messud.dim_location loc ON loc.LocationKey = obs.LocationKey
    WHERE org.Type = 'pest'
      AND org.Name = 'Nosema'
    GROUP BY loc.County
    ORDER BY avg_nosema ASC
)
SELECT 
  l.County AS county,
  l.avg_nosema AS average_nosema,
  SUM(obs.Occurrence) AS bee_occurrences
FROM low_nosema l
JOIN messud.dim_location loc ON loc.County = l.County
JOIN messud.fact_observations obs ON obs.LocationKey = loc.LocationKey
JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
WHERE org.Type = 'bee'
GROUP BY l.County, l.avg_nosema
ORDER BY l.avg_nosema ASC
limit 5;

-- How many bee occurrences are there in the 5 counties with the most Nosema fungus?

WITH low_nosema AS (
    SELECT
        loc.County,
        AVG(obs.Occurrence) AS avg_nosema
    FROM messud.fact_observations obs
    JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
    JOIN messud.dim_location loc ON loc.LocationKey = obs.LocationKey
    WHERE org.Type = 'pest'
      AND org.Name = 'Nosema'
    GROUP BY loc.County
    ORDER BY avg_nosema desc
)
SELECT 
  l.County AS county,
  l.avg_nosema AS average_nosema,
  SUM(obs.Occurrence) AS bee_occurrences
FROM low_nosema l
JOIN messud.dim_location loc ON loc.County = l.County
JOIN messud.fact_observations obs ON obs.LocationKey = loc.LocationKey
JOIN messud.dim_organism org ON org.OrganismKey = obs.OrganismKey
WHERE org.Type = 'bee'
GROUP BY l.County, l.avg_nosema
ORDER BY l.avg_nosema desc
limit 5;

--Question about bees’ safety per county:
--Which county is most popular for beekeeping and which is most safe from pests?

WITH bee_popularity AS (
  SELECT
    loc.County,
    SUM(obs.Occurrence) AS bee_count
  FROM messud.fact_observations obs
  JOIN messud.dim_organism  org ON org.OrganismKey = obs.OrganismKey
  JOIN messud.dim_location loc ON loc.LocationKey = obs.LocationKey
  WHERE org.Type = 'bee'
  GROUP BY loc.County
),

pest_levels AS (
  SELECT
    loc.County,
    AVG(CASE WHEN org.Name = 'Varroa' THEN obs.Occurrence ELSE 0 END) AS avg_varroa,
    AVG(CASE WHEN org.Name = 'Nosema' THEN obs.Occurrence ELSE 0 END) AS avg_nosema,
    SUM(CASE WHEN org.Type = 'virus' THEN obs.Occurrence ELSE 0 END)       AS virus_sum
  FROM messud.fact_observations obs
  JOIN messud.dim_organism  org ON org.OrganismKey = obs.OrganismKey
  JOIN messud.dim_location loc ON loc.LocationKey = obs.LocationKey
  WHERE org.Type IN ('pest', 'virus')
  GROUP BY loc.County
)

SELECT
  bee.County,
  bee.bee_count,
  pest.avg_varroa,
  pest.avg_nosema,
  pest.virus_sum,
  (pest.avg_varroa + pest.avg_nosema + pest.virus_sum) AS pest_score
FROM bee_popularity bee
JOIN pest_levels pest USING (County)
ORDER BY bee.bee_count DESC, pest_score ASC;