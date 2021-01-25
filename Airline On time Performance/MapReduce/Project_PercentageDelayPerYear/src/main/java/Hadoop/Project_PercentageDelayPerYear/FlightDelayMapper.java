package Hadoop.Project_PercentageDelayPerYear;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FlightDelayMapper extends Mapper<LongWritable,Text,Text,FlightDelayCustomWritable>{
	private FlightDelayCustomWritable outValue = new FlightDelayCustomWritable();
	private Text outKey = new Text();
	private double delay;
	
	public void map(LongWritable key,Text value,Context context) {
		try {
			if(key.get() == 0 && value.toString().contains("header")) {
				return;
			}
			else {	
				String[] tokens = value.toString().split(",");
				String year = tokens[0];
				if(tokens[14].isEmpty()||tokens[14].equalsIgnoreCase("NA")) {
					 delay = 0.0;
				}
				else {
					delay = 1.0;
				}
				outKey.set(year);
				outValue.setTotalDelay(delay);
				outValue.setTotalCount(1.0);
				
				context.write(outKey,outValue);
			}
		}
		catch (Exception e) {
	        e.printStackTrace();
	    }
		
	}

	}
	
