package Hadoop.AccessLogs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IPReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	int sum = 0;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
			Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException, InterruptedException {
	
	for(IntWritable value : values){
		sum +=value.get();
		}
	IntWritable count = new IntWritable(sum);
	context.write(key,count);
	}
	

}
