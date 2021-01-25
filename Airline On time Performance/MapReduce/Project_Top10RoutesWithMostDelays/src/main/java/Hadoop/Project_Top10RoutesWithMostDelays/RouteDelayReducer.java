package Hadoop.Project_Top10RoutesWithMostDelays;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RouteDelayReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	@Override
	public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		DoubleWritable outValue = new DoubleWritable();
		double sum = 0.0;
		for(DoubleWritable val:values) {
			sum += val.get();
		}
		outValue.set(sum);
		context.write(key, outValue);
		
	}
	
}
