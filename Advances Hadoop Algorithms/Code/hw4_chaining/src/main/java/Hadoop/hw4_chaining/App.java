package Hadoop.hw4_chaining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, InterruptedException
    {
    	Configuration config = new Configuration();
        try {
			Job job = Job.getInstance(config,"Total IP's per Website");
			job.setJarByClass(App.class);
			
			job.setMapperClass(FirstMapper.class);
			job.setCombinerClass(FirstReducer.class);
			job.setReducerClass(FirstReducer.class);
			
			//job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			
			FileSystem fs = FileSystem.get(config);
	 		fs.delete(new Path(args[1]), true);
	 		fs.delete(new Path(args[2]), true);
	 		
			boolean success = job.waitForCompletion(true);
			System.out.println(success);
			
			Job job2 = Job.getInstance(config, "sort by frequency");
		    job2.setJarByClass(App.class);
		    
		    job2.setMapperClass(SecondMapper.class);
		    job2.setReducerClass(SecondReducer.class);
		    
		    //job2.setSortComparatorClass(DecreasingComparator.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(LongWritable.class);
		    
		    job2.setInputFormatClass(TextInputFormat.class);
		    FileInputFormat.addInputPath(job2, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job2,new Path(args[2]));
		    boolean success2 = job2.waitForCompletion(true);
			System.out.println(success2);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
}
