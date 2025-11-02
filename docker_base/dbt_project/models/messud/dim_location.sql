/* Model SQL for dim.Organism.
Joins together data from both GBIF and Aphis datasets. 
For GBIF, staging is not used. In the case of APHIs, the staging model stg_aphis_long, which was used to change the data into long format, will be referenced.

Macro get_state_name() is used to convert state codes into full state names.
*/

/* Selecting the APHIS data. 
The APHIS data should currently only contain data collected from the US, thus there are no columns indicating country or continent.
If the macro does not identify a state as one of the US states, then the country and continent are set as unknown, because then the state may not represent a US state.
*/

WITH aphis_locations AS (
	SELECT 
		 sampling_county AS County,
		 {{get_state_name("state_code")}} AS State, -- Using the macro get_state_name() to get the full state name.
		CASE
		 	WHEN state_code IS NULL OR {{ get_state_name("state_code") }} = 'Unknown' THEN 'Unknown' -- If State is unknown, Continent will be set as "Unknown", otherwise it will be "North America".
			ELSE 'North America'
		END AS Continent,
		CASE
			WHEN state_code IS NULL OR {{ get_state_name("state_code") }} = 'Unknown' THEN 'Unknown' -- If State is unknown, Country will be set as "Unknown", otherwise it will be "US"
			ELSE 'US'
		END AS Country
	FROM {{ref('stg_aphis_long') }}
	GROUP BY County, State, Country, Continent -- Grouping the data together.
),

-- Selecting the GBIF location data
gbif_locations AS (
	SELECT 
		replaceAll(continent,'_', ' ') AS Continent, 
		countryCode AS Country,
		stateProvince AS State,
		county AS County,
		decimalLatitude AS Latitude,
		decimalLongitude AS Longitude
	FROM {{ref('stg_gbif') }}
	GROUP BY County, State, Country, Continent, decimalLatitude, decimalLongitude
),

-- Combining the two datasets together.
locationsCombined AS (
	SELECT Continent, Country, State, County, NULL AS Latitude, NULL AS Longitude FROM aphis_locations
	UNION
	SELECT Continent, Country, State, County, Latitude, Longitude from gbif_locations
)

-- Extracting the required data for dim.location.
SELECT
	{{ dbt_utils.generate_surrogate_key(['Continent','Country', 'State', 'County']) }} AS LocationKey,
	Continent,
	Country,
	State,
	County,
	Latitude,
	Longitude 
FROM locationsCombined

ORDER BY Continent, Country, State, County
