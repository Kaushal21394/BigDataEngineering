package Hadoop.NYSE_avg_stock_price;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class App2
{
    public static void main( String[] args ) throws ClassNotFoundException, InterruptedException
    {
    	Configuration config = new Configuration();
    	try {
			Job job = Job.getInstance(config,"Avg Value Per Stock");
			job.setJarByClass(App2.class);
			
			job.setMapperClass(AverageMapper.class);
			//job.setCombinerClass(AveragePriceCombiner.class);
			job.setReducerClass(AveragePriceReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			FileInputFormat.addInputPath(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));

			boolean success = job.waitForCompletion(true);
			System.out.println(success);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
}
