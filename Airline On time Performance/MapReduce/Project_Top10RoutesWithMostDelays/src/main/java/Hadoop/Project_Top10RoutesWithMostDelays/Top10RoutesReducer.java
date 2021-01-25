package Hadoop.Project_Top10RoutesWithMostDelays;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Top10RoutesReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

	private TreeMap<Double,Text> outRecords = new TreeMap<Double,Text>(Collections.reverseOrder());
	
	public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		
		double outResult = 0.0;
		
		for(DoubleWritable value : values) {
			outResult = value.get();
			
		}
		outRecords.put(outResult, new Text(key));
			
		if(outRecords.size()>10) {
			outRecords.remove(outRecords.firstKey());
		}
	
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

	for (Map.Entry<Double, Text> entry : outRecords.entrySet())  
    { 
        double count = entry.getKey(); 
        Text name = entry.getValue(); 

        context.write(name, new DoubleWritable(count)); 
    } 

	}
	
}
