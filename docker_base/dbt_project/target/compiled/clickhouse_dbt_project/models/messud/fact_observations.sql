/* SQL for the fact table Observations
Joins together data from both GBIF and Aphis datasets using the established models. 
For GBIF, staging is not used. In the case of APHIs, the staging model stg_aphis_long, which was used to change the data into long format, will be referenced.
*/

-- Selecting the GBIF data using the established model.
SELECT
	o.OrganismKey AS OrganismKey,
	l.LocationKey AS LocationKey,
	d.DateKey AS DateKey,
	toFloat64(gbif.individualCount) AS Occurrence
FROM `messud`.`stg_gbif` AS gbif
LEFT JOIN `messud`.`dim_organism` AS o
	ON gbif.scientificName = o.ScientificName
	AND o.is_current = 1
LEFT JOIN `messud`.`dim_location` AS l
	ON gbif.decimalLatitude = l.Latitude
	AND gbif.decimalLongitude = l.Longitude
LEFT JOIN `messud`.`dim_date` AS d
	ON toDate(parseDateTimeBestEffort(splitByChar('/', gbif.eventDate)[1])) = d.FullDate
UNION DISTINCT

-- Selecting the APHIS data using the established model.

SELECT
	o.OrganismKey AS OrganismKey,
	l.LocationKey AS LocationKey,
	d.DateKey AS DateKey,
	toFloat64(aphis.value) AS Occurrence
FROM `messud`.`stg_aphis_long` AS aphis
LEFT JOIN `messud`.`dim_organism` AS o
	ON aphis.pest_name = o.Name
	AND o.is_current = 1
LEFT JOIN `messud`.`dim_location` AS l
	ON aphis.sampling_county = l.County
LEFT JOIN `messud`.`dim_date` AS d
	ON makeDate(aphis.sample_year, aphis.sample_month_number, 1) = d.FullDate