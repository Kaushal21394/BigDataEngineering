package Hadoop.Project_BestTimeToFly;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class YearlyTrafficMapper extends Mapper<LongWritable,Text,Text,CustomWritable> {
	
	private CustomWritable outValue = new CustomWritable();
	private Text outKey = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		if (!value.toString().contains("UniqueCarrier")) {
        	if(!tokens[14].equalsIgnoreCase("NA")){
        		String airport = tokens[16]; 
        		int year = Integer.parseInt(tokens[0]);
        		
        		String obj = String.valueOf(year)+"\t"+airport;
        		outKey.set(obj);
  
        		outValue.setArrDelay(Integer.parseInt(tokens[14]));
        		outValue.setCancelled(Integer.parseInt(tokens[21]));
        		outValue.setDiverted(Integer.parseInt(tokens[23]));
        		outValue.setFlightCount(1);
        		
        		int taxin;
        		int taxout;
        		
        		if(tokens[19].contains("NA")||tokens[19].isEmpty()) {
        			taxin = 0;
        		}
        		else {
        			taxin = Integer.parseInt(tokens[19]);
        		}
        		
        		if(tokens[20].contains("NA")||tokens[20].isEmpty()) {
        			taxout = 0;
        		}
        		else {
        			taxout = Integer.parseInt(tokens[20]);
        		}
        		
        		outValue.setTaxing(taxin+taxout);
        		context.write(outKey, outValue);
        	}
	}
	}

}
