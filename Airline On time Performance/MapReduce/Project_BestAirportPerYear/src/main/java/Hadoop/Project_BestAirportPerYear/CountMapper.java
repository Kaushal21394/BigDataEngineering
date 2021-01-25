package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable,Text,Text,FlightAnalysisCustomWritable> {
	
	private FlightAnalysisCustomWritable outValue = new FlightAnalysisCustomWritable();
	private Text outKey = new Text();
	
	private int arrDelay = 0;
	private int depDelay = 0;
	private int carrierDelay = 0;
	private int weatherDelay = 0;
	private int nasDelay = 0;
	private int securityDelay = 0;
	private int lateDelay = 0;
	private int totalDelay = 0;
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		if (!value.toString().contains("UniqueCarrier")) {

			String[] tokens = value.toString().split(",");
			String year = tokens[0];
			String airport = tokens[16];
			
			if(!(tokens[14].contains("NA")||tokens[14].isEmpty())) {
				arrDelay = Integer.parseInt(tokens[14]);
			}
			if(!(tokens[15].contains("NA")||tokens[15].isEmpty())) {
				depDelay = Integer.parseInt(tokens[15]);
			}
			if(!(tokens[24].contains("NA")||tokens[24].isEmpty())) {
				carrierDelay = Integer.parseInt(tokens[24]);
			}
			if(!(tokens[25].contains("NA")||tokens[25].isEmpty())) {
				weatherDelay = Integer.parseInt(tokens[25]);
			}
			if(!(tokens[26].contains("NA")||tokens[26].isEmpty())) {
				nasDelay = Integer.parseInt(tokens[26]);
			}
			if(!(tokens[27].contains("NA")||tokens[27].isEmpty())) {
				securityDelay = Integer.parseInt(tokens[27]);
			}
			if(!(tokens[28].contains("NA")||tokens[28].isEmpty())) {
				lateDelay = Integer.parseInt(tokens[28]);
			}
			
			totalDelay = arrDelay + depDelay + carrierDelay + weatherDelay + nasDelay + securityDelay + lateDelay;
			
			outKey.set(year + "\t" + airport);
			outValue.setTotalDelay(totalDelay);
			outValue.setTotalTrips(1);
			
			context.write(outKey, outValue);
	}
		}
	
}
