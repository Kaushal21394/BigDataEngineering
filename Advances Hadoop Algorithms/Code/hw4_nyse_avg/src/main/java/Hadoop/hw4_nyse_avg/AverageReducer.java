package Hadoop.hw4_nyse_avg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<IntWritable, CompositeKeyWritable, IntWritable, CompositeKeyWritable> {

	CompositeKeyWritable result = new CompositeKeyWritable();
	
	@Override
	protected void reduce(IntWritable key, Iterable<CompositeKeyWritable> values, Context context) throws IOException, InterruptedException{
		double sum = 0.0;
		long count = 0;
		
		for (CompositeKeyWritable value : values) {
			
			sum += value.getCount() * value.getLocal_avg();
			count += value.getCount();
		}
		
		result.setCount(count);
		result.setLocal_avg(sum /count);

		context.write(key, result);
	}
}