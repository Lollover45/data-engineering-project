--makeDate() viimane argument on 1, mis on decoy paevaarv
WITH aphis_dates AS (
	SELECT 
		makeDate(sample_year, sample_month_number, 1) AS fullDate, 
		sample_year AS year, 
		sample_month_number AS month 
	FROM {{source('messud','APHIS_records')}}
	GROUP BY sample_year, sample_month_number
),

gbif_dates AS (
	SELECT 
		toDate(parseDateTimeBestEffort(splitByChar('/', eventDate)[1])) AS fullDate, 
		year, 
		month
	FROM {{source('messud','gbif')}}
	GROUP BY year, month, fullDate
),

datesCombined AS (
	select fullDate, year, month from aphis_dates
	UNION ALL
	select fullDate, year, month from gbif_dates
)
SELECT
	fullDate,
	year,
	month
FROM datesCombined

ORDER BY fullDate