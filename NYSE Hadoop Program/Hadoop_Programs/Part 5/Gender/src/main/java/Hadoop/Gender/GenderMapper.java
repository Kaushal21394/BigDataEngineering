package Hadoop.Gender;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderMapper extends Mapper <Text,Text,Text,IntWritable> {
	IntWritable count = new IntWritable(1);
	
	public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
		String line = key.toString();
		context.write(new Text(line), count);
	}

	
	
}
