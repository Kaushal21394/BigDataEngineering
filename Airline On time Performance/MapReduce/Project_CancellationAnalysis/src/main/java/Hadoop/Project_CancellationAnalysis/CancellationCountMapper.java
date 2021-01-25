package Hadoop.Project_CancellationAnalysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CancellationCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
	private LongWritable outValue = new LongWritable(1);
	private Text outKey = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(","); 
	    if (!value.toString().contains("UniqueCarrier")) {
	        if(tokens[21].equalsIgnoreCase("1")){
		        String selectedValue = String.join("\t", Arrays.asList(tokens[0],tokens[8],tokens[21],tokens[22]));
		        outKey.set(selectedValue);
		        
		        //outKey.setYear(tokens[0]);
		        //outKey.setCarrier(tokens[1]);
		        //outKey.setCancellationCode(tokens[3]);
			
			context.write(outKey, outValue);
	}

}
}
	}
