
  
    
    
    
        
         


        insert into `messud`.`dim_date__dbt_backup`
        ("DateKey", "FullDate", "Year", "Month", "Season", "Decade")/* Model SQL for dim.Date.
Joins together data from both GBIF and Aphis datasets. 
For GBIF, staging is not used. In the case of APHIs, the staging model stg_aphis_long, which was used to change the data into long format, will be referenced.
*/

-- Selecting the APHIS data
WITH aphis_dates AS (
	SELECT 
		makeDate(sample_year, sample_month_number, 1) AS FullDate, 
		sample_year AS Year, 
		sample_month_number AS Month 
	FROM `messud`.`aphis`
	GROUP BY sample_year, sample_month_number
),

-- Selecting the GBIF data
gbif_dates AS (
	SELECT 
		toDate(parseDateTimeBestEffort(splitByChar('/', eventDate)[1])) AS FullDate, 
		year AS Year, 
		month AS Month
	FROM `messud`.`stg_gbif`
	GROUP BY year, month, FullDate
),

-- Combining the APHIS and GBIF date data
datesCombined AS (
	SELECT FullDate, Year, Month FROM aphis_dates
	UNION ALL
	SELECT FullDate, Year, Month FROM gbif_dates
)

-- Extracting the required data for dim.Date
SELECT
	lower(hex(MD5(toString(coalesce(cast(FullDate as String), '_dbt_utils_surrogate_key_null_') )))) AS DateKey,
	FullDate,
	Year,
	Month,
	CASE -- Establishing a new variable named "Season" 
		WHEN Month IN (12,1,2) THEN 'winter'
		WHEN Month IN (3,4,5) THEN 'spring'
		WHEN Month IN (6,7,8) THEN 'summer'
		ELSE 'autumn'
	END AS Season,
	Year - Year % 10 AS Decade -- Establishing a new variable named "Decade"
FROM datesCombined

ORDER BY FullDate
  