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

filtered_data = FILTER airlineRawData BY Year in ('2005,2006','2007','2008') and Diverted == '1';

records = FOREACH filtered_data GENERATE Origin,Dest,Diverted;

grpd = GROUP records by (Origin,Dest);

counted = FOREACH grpd GENERATE  FLATTEN(group) as (Origin , Dest) , COUNT(records) as diverted_count;

Ordered = ORDER counted by diverted_count Desc;

top15 = LIMIT Ordered  10;

STORE top15 INTO '/Project/pig/TopRoutesWithDiversion' using PigStorage(',');
