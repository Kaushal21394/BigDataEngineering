package Hadoop.Project_PercentageDelayPerYear;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightDelayReducer extends Reducer<Text,FlightDelayCustomWritable,Text,DoubleWritable>{
	
	public void reduce(Text key,Iterable<FlightDelayCustomWritable> value,Context context) throws IOException, InterruptedException {
		FlightDelayCustomWritable fcd = new FlightDelayCustomWritable();
		DoubleWritable outValue = new DoubleWritable();
		double totalDelay = 0.0;
		double totalCount = 0.0;
		double percentage = 0.0;
		
		for(FlightDelayCustomWritable val:value) {
			totalDelay += val.getTotalDelay();
			totalCount += val.getTotalCount();	
		}
		percentage = totalDelay*100/totalCount;
		outValue.set(percentage);
		context.write(key, outValue);
		
	}

}
