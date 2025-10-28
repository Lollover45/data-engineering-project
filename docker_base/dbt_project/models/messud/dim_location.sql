SELECT
    county,
    decimalLatitude,
    decimalLongitude,
FROM {{ source('messud', 'gbif') }}