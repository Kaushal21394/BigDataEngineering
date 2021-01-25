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

filtered_data = FILTER airlineRawData BY Year in ('2005,2006','2007','2008') ;

records = FOREACH filtered_data GENERATE Year, WeatherDelay;
grpd = GROUP records BY Year;
q2 = FOREACH grpd {
    weather_delays = FILTER records BY WeatherDelay > 0;    
    GENERATE group, COUNT(weather_delays); 
};

STORE q2 INTO '/Project/pig/DelayByWeather';