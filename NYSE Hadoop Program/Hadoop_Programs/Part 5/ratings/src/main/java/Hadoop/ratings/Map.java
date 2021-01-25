package Hadoop.ratings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper <LongWritable,Text,Text,IntWritable> {
	IntWritable count = new IntWritable(1);
	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split("::");
		context.write(new Text(tokens[1]), count);
	}

}
