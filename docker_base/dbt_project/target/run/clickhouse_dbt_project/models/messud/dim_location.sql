
  
    
    
    
        
         


        insert into `messud`.`dim_location`
        ("county", "decimalLatitude", "decimalLongitude")SELECT
    county,
    decimalLatitude,
    decimalLongitude,
FROM `messud`.`gbif`
  