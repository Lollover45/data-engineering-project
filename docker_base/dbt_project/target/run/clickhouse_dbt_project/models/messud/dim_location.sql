
  
    
    
    
        
         


        insert into `messud`.`dim_location__dbt_backup`
        ("LocationKey", "Continent", "Country", "State", "County")/* Model SQL for dim.Organism.
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
		 
CASE
    WHEN state_code = 'AL' THEN 'Alabama'
    WHEN state_code = 'AK' THEN 'Alaska'
    WHEN state_code = 'AZ' THEN 'Arizona'
    WHEN state_code = 'AR' THEN 'Arkansas'
    WHEN state_code = 'CA' THEN 'California'
    WHEN state_code = 'CO' THEN 'Colorado'
    WHEN state_code = 'CT' THEN 'Connecticut'
    WHEN state_code = 'DE' THEN 'Delaware'
    WHEN state_code = 'FL' THEN 'Florida'
    WHEN state_code = 'GA' THEN 'Georgia'
    WHEN state_code = 'HI' THEN 'Hawaii'
    WHEN state_code = 'ID' THEN 'Idaho'
    WHEN state_code = 'IL' THEN 'Illinois'
    WHEN state_code = 'IN' THEN 'Indiana'
    WHEN state_code = 'IA' THEN 'Iowa'
    WHEN state_code = 'KS' THEN 'Kansas'
    WHEN state_code = 'KY' THEN 'Kentucky'
    WHEN state_code = 'LA' THEN 'Louisiana'
    WHEN state_code = 'ME' THEN 'Maine'
    WHEN state_code = 'MD' THEN 'Maryland'
    WHEN state_code = 'MA' THEN 'Massachusetts'
    WHEN state_code = 'MI' THEN 'Michigan'
    WHEN state_code = 'MN' THEN 'Minnesota'
    WHEN state_code = 'MS' THEN 'Mississippi'
    WHEN state_code = 'MO' THEN 'Missouri'
    WHEN state_code = 'MT' THEN 'Montana'
    WHEN state_code = 'NE' THEN 'Nebraska'
    WHEN state_code = 'NV' THEN 'Nevada'
    WHEN state_code = 'NH' THEN 'New Hampshire'
    WHEN state_code = 'NJ' THEN 'New Jersey'
    WHEN state_code = 'NM' THEN 'New Mexico'
    WHEN state_code = 'NY' THEN 'New York'
    WHEN state_code = 'NC' THEN 'North Carolina'
    WHEN state_code = 'ND' THEN 'North Dakota'
    WHEN state_code = 'OH' THEN 'Ohio'
    WHEN state_code = 'OK' THEN 'Oklahoma'
    WHEN state_code = 'OR' THEN 'Oregon'
    WHEN state_code = 'PA' THEN 'Pennsylvania'
    WHEN state_code = 'RI' THEN 'Rhode Island'
    WHEN state_code = 'SC' THEN 'South Carolina'
    WHEN state_code = 'SD' THEN 'South Dakota'
    WHEN state_code = 'TN' THEN 'Tennessee'
    WHEN state_code = 'TX' THEN 'Texas'
    WHEN state_code = 'UT' THEN 'Utah'
    WHEN state_code = 'VT' THEN 'Vermont'
    WHEN state_code = 'VA' THEN 'Virginia'
    WHEN state_code = 'WA' THEN 'Washington'
    WHEN state_code = 'WV' THEN 'West Virginia'
    WHEN state_code = 'WI' THEN 'Wisconsin'
    WHEN state_code = 'WY' THEN 'Wyoming'
    ELSE 'Unknown'
END
 AS State, -- Using the macro get_state_name() to get the full state name.
		CASE
		 	WHEN state_code IS NULL OR 
CASE
    WHEN state_code = 'AL' THEN 'Alabama'
    WHEN state_code = 'AK' THEN 'Alaska'
    WHEN state_code = 'AZ' THEN 'Arizona'
    WHEN state_code = 'AR' THEN 'Arkansas'
    WHEN state_code = 'CA' THEN 'California'
    WHEN state_code = 'CO' THEN 'Colorado'
    WHEN state_code = 'CT' THEN 'Connecticut'
    WHEN state_code = 'DE' THEN 'Delaware'
    WHEN state_code = 'FL' THEN 'Florida'
    WHEN state_code = 'GA' THEN 'Georgia'
    WHEN state_code = 'HI' THEN 'Hawaii'
    WHEN state_code = 'ID' THEN 'Idaho'
    WHEN state_code = 'IL' THEN 'Illinois'
    WHEN state_code = 'IN' THEN 'Indiana'
    WHEN state_code = 'IA' THEN 'Iowa'
    WHEN state_code = 'KS' THEN 'Kansas'
    WHEN state_code = 'KY' THEN 'Kentucky'
    WHEN state_code = 'LA' THEN 'Louisiana'
    WHEN state_code = 'ME' THEN 'Maine'
    WHEN state_code = 'MD' THEN 'Maryland'
    WHEN state_code = 'MA' THEN 'Massachusetts'
    WHEN state_code = 'MI' THEN 'Michigan'
    WHEN state_code = 'MN' THEN 'Minnesota'
    WHEN state_code = 'MS' THEN 'Mississippi'
    WHEN state_code = 'MO' THEN 'Missouri'
    WHEN state_code = 'MT' THEN 'Montana'
    WHEN state_code = 'NE' THEN 'Nebraska'
    WHEN state_code = 'NV' THEN 'Nevada'
    WHEN state_code = 'NH' THEN 'New Hampshire'
    WHEN state_code = 'NJ' THEN 'New Jersey'
    WHEN state_code = 'NM' THEN 'New Mexico'
    WHEN state_code = 'NY' THEN 'New York'
    WHEN state_code = 'NC' THEN 'North Carolina'
    WHEN state_code = 'ND' THEN 'North Dakota'
    WHEN state_code = 'OH' THEN 'Ohio'
    WHEN state_code = 'OK' THEN 'Oklahoma'
    WHEN state_code = 'OR' THEN 'Oregon'
    WHEN state_code = 'PA' THEN 'Pennsylvania'
    WHEN state_code = 'RI' THEN 'Rhode Island'
    WHEN state_code = 'SC' THEN 'South Carolina'
    WHEN state_code = 'SD' THEN 'South Dakota'
    WHEN state_code = 'TN' THEN 'Tennessee'
    WHEN state_code = 'TX' THEN 'Texas'
    WHEN state_code = 'UT' THEN 'Utah'
    WHEN state_code = 'VT' THEN 'Vermont'
    WHEN state_code = 'VA' THEN 'Virginia'
    WHEN state_code = 'WA' THEN 'Washington'
    WHEN state_code = 'WV' THEN 'West Virginia'
    WHEN state_code = 'WI' THEN 'Wisconsin'
    WHEN state_code = 'WY' THEN 'Wyoming'
    ELSE 'Unknown'
END
 = 'Unknown' THEN 'Unknown' -- If State is unknown, Continent will be set as "Unknown", otherwise it will be "North America".
			ELSE 'North America'
		END AS Continent,
		CASE
			WHEN state_code IS NULL OR 
CASE
    WHEN state_code = 'AL' THEN 'Alabama'
    WHEN state_code = 'AK' THEN 'Alaska'
    WHEN state_code = 'AZ' THEN 'Arizona'
    WHEN state_code = 'AR' THEN 'Arkansas'
    WHEN state_code = 'CA' THEN 'California'
    WHEN state_code = 'CO' THEN 'Colorado'
    WHEN state_code = 'CT' THEN 'Connecticut'
    WHEN state_code = 'DE' THEN 'Delaware'
    WHEN state_code = 'FL' THEN 'Florida'
    WHEN state_code = 'GA' THEN 'Georgia'
    WHEN state_code = 'HI' THEN 'Hawaii'
    WHEN state_code = 'ID' THEN 'Idaho'
    WHEN state_code = 'IL' THEN 'Illinois'
    WHEN state_code = 'IN' THEN 'Indiana'
    WHEN state_code = 'IA' THEN 'Iowa'
    WHEN state_code = 'KS' THEN 'Kansas'
    WHEN state_code = 'KY' THEN 'Kentucky'
    WHEN state_code = 'LA' THEN 'Louisiana'
    WHEN state_code = 'ME' THEN 'Maine'
    WHEN state_code = 'MD' THEN 'Maryland'
    WHEN state_code = 'MA' THEN 'Massachusetts'
    WHEN state_code = 'MI' THEN 'Michigan'
    WHEN state_code = 'MN' THEN 'Minnesota'
    WHEN state_code = 'MS' THEN 'Mississippi'
    WHEN state_code = 'MO' THEN 'Missouri'
    WHEN state_code = 'MT' THEN 'Montana'
    WHEN state_code = 'NE' THEN 'Nebraska'
    WHEN state_code = 'NV' THEN 'Nevada'
    WHEN state_code = 'NH' THEN 'New Hampshire'
    WHEN state_code = 'NJ' THEN 'New Jersey'
    WHEN state_code = 'NM' THEN 'New Mexico'
    WHEN state_code = 'NY' THEN 'New York'
    WHEN state_code = 'NC' THEN 'North Carolina'
    WHEN state_code = 'ND' THEN 'North Dakota'
    WHEN state_code = 'OH' THEN 'Ohio'
    WHEN state_code = 'OK' THEN 'Oklahoma'
    WHEN state_code = 'OR' THEN 'Oregon'
    WHEN state_code = 'PA' THEN 'Pennsylvania'
    WHEN state_code = 'RI' THEN 'Rhode Island'
    WHEN state_code = 'SC' THEN 'South Carolina'
    WHEN state_code = 'SD' THEN 'South Dakota'
    WHEN state_code = 'TN' THEN 'Tennessee'
    WHEN state_code = 'TX' THEN 'Texas'
    WHEN state_code = 'UT' THEN 'Utah'
    WHEN state_code = 'VT' THEN 'Vermont'
    WHEN state_code = 'VA' THEN 'Virginia'
    WHEN state_code = 'WA' THEN 'Washington'
    WHEN state_code = 'WV' THEN 'West Virginia'
    WHEN state_code = 'WI' THEN 'Wisconsin'
    WHEN state_code = 'WY' THEN 'Wyoming'
    ELSE 'Unknown'
END
 = 'Unknown' THEN 'Unknown' -- If State is unknown, Country will be set as "Unknown", otherwise it will be "US"
			ELSE 'US'
		END AS Country
	FROM `messud`.`stg_aphis_long`
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
	FROM `messud`.`stg_gbif`
	GROUP BY County, State, Country, Continent, decimalLatitude, decimalLongitude
),

-- Combining the two datasets together.
locationsCombined AS (
	SELECT Continent, Country, State, County, NULL AS Latitude, NULL AS Longitude FROM aphis_locations
	UNION ALL
	SELECT Continent, Country, State, County, Latitude, Longitude from gbif_locations
)

-- Extracting the required data for dim.location.
SELECT
	lower(hex(MD5(toString(coalesce(cast(Continent as String), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as String), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(State as String), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(County as String), '_dbt_utils_surrogate_key_null_') )))) AS LocationKey,
	Continent,
	Country,
	State,
	County
FROM locationsCombined

ORDER BY Continent, Country, State, County
  