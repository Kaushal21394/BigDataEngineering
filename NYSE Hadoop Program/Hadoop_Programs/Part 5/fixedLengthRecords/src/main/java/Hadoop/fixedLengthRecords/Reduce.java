package Hadoop.fixedLengthRecords;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

	protected void reduce(Text key, Iterable<IntWritable> values, 
			Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException, InterruptedException{
	int sum = 0;
	for (IntWritable val : values) {
		if(val.get()==7) {
		sum += val.get();
	}
	}
		context.write(key, new IntWritable(sum));
	}
}