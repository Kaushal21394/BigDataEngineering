package Hadoop.fixedLengthRecords;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper <LongWritable,BytesWritable,Text,IntWritable> {
	
	public void map(LongWritable key,BytesWritable value, Context context) throws IOException, InterruptedException {

		String line_val = key.toString();
		String [] val = line_val.split("");
		context.write(new Text(line_val), new IntWritable(1));
	}

}