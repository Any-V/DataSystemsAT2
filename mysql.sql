CREATE DATABASE IF NOT EXISTS Suburb_Price;

use Suburb_Price; 
CREATE TABLE Suburb_Price(
Price int(255), 
Date_sold date, 
Suburb varchar(255),
Num_Bath numeric(255), 
Num_bed numeric(255), 
num_parking numeric (255), 
property_size numeric (255), 
type varchar(255), 
suburb_population numeric(255), 
Suburb_median_income numeric(255), 
Property_inflation_index numeric(255.10), 
km_from_cbd(255.10) );
