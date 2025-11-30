
  
    
    
    
        
         


        insert into `messud`.`stg_gbif`
        ("gbifID", "eventDate", "year", "month", "day", "individualCount", "continent", "countryCode", "stateProvince", "county", "decimalLatitude", "decimalLongitude", "scientificName", "species")SELECT DISTINCT *
FROM `messud`.`gbif`
  