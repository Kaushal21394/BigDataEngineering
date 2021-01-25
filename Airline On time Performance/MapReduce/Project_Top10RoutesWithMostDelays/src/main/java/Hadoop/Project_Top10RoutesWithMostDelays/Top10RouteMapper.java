package Hadoop.Project_Top10RoutesWithMostDelays;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10RouteMapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
	private TreeMap<Double,Text> records = new TreeMap<Double,Text>();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		records.put(Double.parseDouble(tokens[1].trim()), new Text(tokens[0].trim()));
		
		if(records.size()>10) {
			records.remove(records.firstKey());
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Double, Text> entry : records.entrySet())  
        { 
  
            double count = entry.getKey(); 
            Text name = entry.getValue(); 
  
            context.write(name, new DoubleWritable(count)); 
        } 
  
	}

}
