package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportMapper extends Mapper<LongWritable,Text,Text,Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		
	
	if(key.get() > 0) {
		String[] tokens = value.toString().replace("\"", "").split(",");
		if(!(tokens[0].contains("NA")||tokens[0].isEmpty())) {
		
			outKey.set(tokens[0]);
			outValue.set(String.join("\t",Arrays.asList("B",tokens[0],tokens[1])));
			context.write(outKey, outValue);
		}
	}
	}
	
}
