package Hadoop.hw4_chaining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	IntWritable one = new IntWritable(1);
	Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

	String line = value.toString();
	String [] tokens = line.split("- -");

	
	word.set(tokens[0]);
	context.write(word,one);
		

	}


	

}