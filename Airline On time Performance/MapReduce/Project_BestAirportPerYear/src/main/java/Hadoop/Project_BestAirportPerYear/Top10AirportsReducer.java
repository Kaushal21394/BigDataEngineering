package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10AirportsReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {

	private TreeMap<Float,Text> outRecords = new TreeMap<Float,Text>();
	
	public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
		
		float outResult = 0;
		
		for(FloatWritable value : values) {
			outResult = value.get();
			
		}
		outRecords.put(outResult,new Text(key));
			
		if(outRecords.size()>10) {
			outRecords.remove(outRecords.firstKey());
		}
	
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

	for (Map.Entry<Float, Text> entry : outRecords.entrySet())  
    { 
        float count = entry.getKey(); 
        Text name = entry.getValue(); 

        context.write(name, new FloatWritable(count)); 
    } 

	}
}
