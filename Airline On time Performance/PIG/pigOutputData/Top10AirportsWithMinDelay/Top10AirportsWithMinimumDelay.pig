airlineRawData = LOAD '/AirArrivalDataSet/' 
USING PigStorage(',') AS (Year:chararray, Month:chararray, DayofMonth:chararray, 
DayOfWeek:chararray, DepTime:chararray, CRSDepTime:chararray, ArrTime:chararray, 
CRSArrTime:chararray, UniqueCarrier:chararray, FlightNum:chararray, 
TailNum:chararray, ActualElapsedTime:chararray, CRSElapsedTime:chararray, 
AirTime:chararray, ArrDelay:int, DepDelay:int, Origin:chararray, Dest:chararray, 
Distance:chararray, TaxiIn:chararray, TaxiOut:chararray, Cancelled:chararray, 
CancellationCode:chararray, Diverted:chararray, CarrierDelay:chararray, 
WeatherDelay:int, NASDelay:chararray, SecurityDelay:chararray, 
LateAircraftDelay:chararray); 

filtered_data = FILTER airlineRawData BY Year == '2008' AND (DepDelay is not null OR ArrDelay is not null );

carrier = LOAD '/Carrier/'
USING PigStorage(',') AS (iata:chararray,CarrierName:chararray);

carrierData = FOREACH carrier GENERATE REPLACE(iata,'\\"','') as iata,REPLACE(CarrierName,'\\"','') as CarrierName;

filter_carrierData = FILTER carrierData by (iata is not null) AND (CarrierName is not null) ;

mergedData = JOIN filtered_data by UniqueCarrier, filter_carrierData by iata;

records = FOREACH mergedData GENERATE CarrierName, DepDelay;
grpd = GROUP records BY CarrierName;
counts = FOREACH grpd GENERATE group AS CarrierName, 
                               COUNT(records) AS DelayCount;
sorted = ORDER counts BY DelayCount DESC;
q3_1 = LIMIT sorted 10;

STORE q3_1 INTO '/Project/pig/Top10AirportsWithMinimumDelay';