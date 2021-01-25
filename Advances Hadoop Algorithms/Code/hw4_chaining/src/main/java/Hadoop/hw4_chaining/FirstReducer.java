package Hadoop.hw4_chaining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	//int sum = 0;
	IntWritable count = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
			Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value : values){
		sum +=value.get();
		}
	count.set(sum);
	context.write(key,count);
	}
	
}
