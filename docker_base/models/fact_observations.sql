SELECT
	o.organism_key AS OrganismKey,
	l.location_key AS LocationKey,
	d.date_key AS DateKey,
	gbif.individualCount AS Occurrence
FROM {{ source('messud', 'gbif') }} AS gbif
LEFT JOIN {{ ref('dim_organism') }} AS o
	ON gbif.scientificName = o.ScientificName
LEFT JOIN {{ ref('dim_location') }} AS l
	ON gbif.county = l.County
LEFT JOIN {{ ref('dim_date') }} AS d
	ON toDate(parseDateTimeBestEffort(splitByChar('/', gbif.date)[1])) = d.FullDate
UNION ALL
SELECT
	o.organism_key AS OrganismKey,
	l.location_key AS LocationKey,
	d.date_key AS DateKey,
	aphis.value AS Occurrence
FROM {{ ref('stg_aphis_long') }} AS aphis
LEFT JOIN {{ ref('dim_organism') }} AS o
	ON aphis.pest_name = o.Name
LEFT JOIN {{ ref('dim_location') }} AS l
	ON aphis.sampling_county = l.County
LEFT JOIN {{ ref('dim_date') }} AS d
	ON makeDate(aphis.sample_year, aphis.sample_month_number, 1) = d.FullDate
