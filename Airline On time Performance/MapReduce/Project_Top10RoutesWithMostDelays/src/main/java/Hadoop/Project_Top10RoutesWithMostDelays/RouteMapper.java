package Hadoop.Project_Top10RoutesWithMostDelays;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RouteMapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
	private Text outKey = new Text();
	private DoubleWritable outValue = new DoubleWritable(0.0);
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		try {
			if(key.get() == 0 && value.toString().contains("header")) {
				return;
			}
			else {
			String[] tokens = value.toString().split(",");
			if(!(tokens[16].isEmpty()||tokens[16].equalsIgnoreCase("NA")) && !(tokens[17].isEmpty()||tokens[17].equalsIgnoreCase("NA"))){
				String arivalAirport = tokens[16].trim();
				String depAirport = tokens[17].trim();
				String combinedPath = arivalAirport +"-" + depAirport;
				double arrDelay;
				double depDelay;
				if(tokens[14].contains("NA")) {
					arrDelay = 0.0;
				}
				else {
					arrDelay = Double.parseDouble(tokens[14]);
				}
				if(tokens[15].contains("NA")) {
					depDelay = 0.0;
				}
				else {
					depDelay = Double.parseDouble(tokens[15]);
				}
				double totalDelay = arrDelay+depDelay; 
			
				outKey.set(combinedPath);
				outValue.set(totalDelay);
				
				context.write(outKey, outValue);
			}
			}
		}
		
			catch (Exception e) {
		        e.printStackTrace();
		    }

	}

}
