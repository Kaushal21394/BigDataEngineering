package Hadoop.Project_CancellationAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class App 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	Configuration conf = new Configuration();
    	
    	Job job1 = Job.getInstance(conf, "Total Counts for Flights that were Cancelled");
        job1.setJarByClass(App.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
 		job1.setOutputFormatClass(TextOutputFormat.class);
 		
        job1.setMapperClass(CancellationCountMapper.class);  
        job1.setReducerClass(CancellationCountReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        FileSystem fs = FileSystem.get(conf);
    	fs.delete(new Path(args[1]), true);
    	
 		boolean success_1 = job1.waitForCompletion(true);
 		System.out.println(success_1);
 		
 		Job job2 = Job.getInstance(conf, "Cancellation Binning ");
        job2.setJarByClass(App.class);
        
        job2.setMapperClass(CancellationReasonBinningMapper.class);

        MultipleOutputs.addNamedOutput(job2, "bins", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.setCountersEnabled(job2, true);
        
        job2.setNumReduceTasks(0);
         
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
        fs.delete(new Path(args[2]), true);
        boolean success_2 = job2.waitForCompletion(true);
 		System.out.println(success_2);
 		}
    }

