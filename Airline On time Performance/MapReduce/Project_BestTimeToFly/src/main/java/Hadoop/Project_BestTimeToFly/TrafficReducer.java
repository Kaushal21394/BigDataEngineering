package Hadoop.Project_BestTimeToFly;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficReducer extends Reducer<Text,CustomWritable,Text,CustomWritable> {
	
	public void reduce(Text key,Iterable<CustomWritable> values,Context context) throws IOException, InterruptedException {
		int arrDelay = 0;
		int flightCount =0;
		int cancelled = 0;
		int diverted = 0;
		int totaltaxing = 0;
		
		CustomWritable outValue = new CustomWritable();
		for(CustomWritable value :values) {
			arrDelay += value.getArrDelay();
			flightCount += value.getFlightCount();
			cancelled += value.getCancelled();
			diverted += value.getDiverted();
			totaltaxing += value.getTaxing();
		}
		
		outValue.setArrDelay(arrDelay);
		outValue.setCancelled(cancelled);
		outValue.setDiverted(diverted);
		outValue.setFlightCount(flightCount);
		outValue.setTaxing(totaltaxing);
		
		context.write(key, outValue);
	}

}
