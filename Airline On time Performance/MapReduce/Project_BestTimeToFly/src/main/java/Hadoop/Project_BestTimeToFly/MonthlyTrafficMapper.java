package Hadoop.Project_BestTimeToFly;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MonthlyTrafficMapper extends Mapper<LongWritable,Text,Text,CustomWritable> {
	
	private CustomWritable outValue = new CustomWritable();
	private Text outKey = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		if (!value.toString().contains("UniqueCarrier")) {
        	if(!tokens[14].equalsIgnoreCase("NA")){
        		String airport = tokens[16]; 
        		int monthNum = Integer.parseInt(tokens[1]);
        		String monthName;
        		switch(monthNum) {
        		case 1:
        			monthName = "January";
        			break;
        		case 2:
        			monthName = "February";
        			break;
        		case 3:
        			monthName = "March";
        			break;
        		case 4:
        			monthName = "April";
        			break;
        		case 5:
        			monthName = "May";
        			break;
        		case 6:
        			monthName = "June";
        			break;
        		case 7:
        			monthName = "July";
        			break;
        		case 8:
        			monthName = "August";
        			break;
        		case 9:
        			monthName = "September";
        			break;
        		case 10:
        			monthName = "October";
        			break;
        		case 11:
        			monthName = "November";
        			break;
        		case 12:
        			monthName = "December";
        			break;
        				
        		default:
        			monthName ="Unknown";
        			break;
        		}
        		
        		        		
        		String obj = String.valueOf(monthNum)+"\t"+airport+"\t"+monthName;
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
