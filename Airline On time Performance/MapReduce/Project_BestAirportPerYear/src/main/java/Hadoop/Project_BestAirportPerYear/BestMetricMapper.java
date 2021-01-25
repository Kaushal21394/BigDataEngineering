package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BestMetricMapper extends Mapper<LongWritable,Text,Text,Text>{
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		String year = tokens[0];
		String airport = tokens[1];
		String metric = tokens[2];
		
		outKey.set(airport);
		outValue.set(String.join("\t",Arrays.asList("A",airport,year,metric)));
		context.write(outKey, outValue);
	}

}
