package Hadoop.Project_CancellationAnalysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CancellationReasonBinningMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

	private MultipleOutputs<Text,NullWritable> multipleOutputs = null;
	
	@Override 
	public void setup(Context context)throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs(context);	
	}
	
	public void map(LongWritable key, Text value, Context context) 
	        throws IOException, InterruptedException {
	  
	    String[] tokens = value.toString().split("\t"); 
        String cancellationCode = tokens[3];
        String selectedValue = String.join(",", Arrays.asList(tokens[0],tokens[1],tokens[2],tokens[4]));
        
        if(cancellationCode.equalsIgnoreCase("A"))
        	multipleOutputs.write("bins", selectedValue + "," + "Carrier-cancellation", NullWritable.get(),"Carrier-cancellation");
        if(cancellationCode.equalsIgnoreCase("B"))
        	multipleOutputs.write("bins", selectedValue+ "," + "Weather-cancellation", NullWritable.get(),"Weather-cancellation");
        if(cancellationCode.equalsIgnoreCase("C"))
        	multipleOutputs.write("bins", selectedValue + "," + "NAS-cancellation", NullWritable.get(),"NAS-cancellation");
        if(cancellationCode.equalsIgnoreCase("D"))
        	multipleOutputs.write("bins", selectedValue + "," + "Security-cancellation", NullWritable.get(),"Security-cancellation");
        else
        	multipleOutputs.write("bins", selectedValue + "," + "Unknown-cancellation", NullWritable.get(),"Unknown-cancellation");
    }	        	   
		        

	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	multipleOutputs.close();
	    }

	}


