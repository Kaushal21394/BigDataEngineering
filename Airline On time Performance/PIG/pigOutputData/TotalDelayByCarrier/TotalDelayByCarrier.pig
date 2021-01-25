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

filtered_data = FILTER airlineRawData BY Year == '2008';

carrier = LOAD '/Carrier/'
USING PigStorage(',') AS (iata:chararray,CarrierName:chararray);

carrierData = FOREACH carrier GENERATE REPLACE(iata,'\\"','') as iata,REPLACE(CarrierName,'\\"','') as CarrierName;

filter_carrierData = FILTER carrierData by (iata is not null) AND (CarrierName is not null) ;

mergedData = JOIN filtered_data by UniqueCarrier, filter_carrierData by iata;

records = FOREACH mergedData GENERATE Month,CarrierName,FlightNum,Origin,
            (ArrDelay + DepDelay) AS SumDelay;
            
grpd = GROUP records BY Month;

top_delays = FOREACH grpd {  
 sum_delays = ORDER records BY SumDelay DESC;
 sum_delays_top1 = LIMIT sum_delays 1;
 GENERATE group AS Month,FLATTEN(sum_delays_top1.CarrierName) AS Carrier,FLATTEN(sum_delays_top1.Origin) AS Airport , FLATTEN(sum_delays_top1.SumDelay) AS SumDelay;};

STORE top_delays INTO '/Project/pig/top10_delay' USING PigStorage(',');