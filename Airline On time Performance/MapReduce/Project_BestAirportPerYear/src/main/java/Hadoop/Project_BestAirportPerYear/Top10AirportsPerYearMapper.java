package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10AirportsPerYearMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
	private TreeMap<Text,Float> records = new TreeMap<Text,Float>();
	private Text outKey = new Text();
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		String airport = tokens[6].trim();
		String airportToken = tokens[5].trim();
		String year = tokens[2].trim();
		float metric = Float.parseFloat(tokens[3].trim());
		
		String outKey = String.join("\t", Arrays.asList(year,airportToken,airport));
		//outKey.setYear(year);
		//outKey.setAirport(airport+"\t"+airportToken);
		records.put(new Text(outKey),metric);

	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Text, Float> entry : records.entrySet())  
        { 
  
            float count = entry.getValue(); 
            Text name = entry.getKey(); 
  
            context.write(name, new FloatWritable(count)); 
        } 
  
	}
}
