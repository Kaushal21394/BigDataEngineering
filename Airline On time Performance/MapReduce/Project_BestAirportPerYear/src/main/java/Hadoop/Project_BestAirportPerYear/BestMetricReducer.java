package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BestMetricReducer extends Reducer<Text,FlightAnalysisCustomWritable,Text,FloatWritable> {
	
	public void reduce(Text key,Iterable<FlightAnalysisCustomWritable> value,Context context) throws IOException, InterruptedException {
		Text outKey = new Text();
		FloatWritable outValue = new FloatWritable();
		int sum = 0;
		int count = 0;
		float metric = 0;
		
		for (FlightAnalysisCustomWritable val : value) {
			sum += val.getTotalDelay();
			count+= val.getTotalTrips();
		}
		
		metric = sum/count;
		
		outKey.set(key);
		outValue.set(metric);
		
		context.write(outKey, outValue);
	}

}
