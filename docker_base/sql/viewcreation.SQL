SELECT
    o.Name AS OrganismName,
    o.ScientificName,
    o.Type,
    o.is_current,
    o.Unit,
    
    l.Country,
    l.County,
    l.State,
    l.Country,
    l.Latitude,
    l.Longitude,

    d.FullDate,
    d.Year,
    d.Month,
    d.Season,
    
    f.Occurrence

FROM fact_observations f
LEFT JOIN dim_organism o   ON f.OrganismKey = o.OrganismKey
LEFT JOIN dim_location l   ON f.LocationKey = l.LocationKey
LEFT JOIN dim_date d       ON f.DateKey = d.DateKey
