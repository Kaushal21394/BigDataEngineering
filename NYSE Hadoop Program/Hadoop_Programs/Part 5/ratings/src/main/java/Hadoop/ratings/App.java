package Hadoop.ratings;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, InterruptedException
    {
    	Configuration config = new Configuration();
    	config.setInt(NLineInputFormat.LINES_PER_MAP, 10000);
    	try {
			Job job = Job.getInstance(config,"Rated Movie Count");
			job.setJarByClass(App.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setInputFormatClass(NLineInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
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
