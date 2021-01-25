package Hadoop.hw4_nyse_avg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
		Configuration conf = new Configuration(true);

		Job job = new Job(conf, "AverageStockPrice");
		job.setJarByClass(App.class);

		job.setMapperClass(AverageMapper.class);
		job.setCombinerClass(AverageReducer.class);
		job.setReducerClass(AverageReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(CompositeKeyWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
 		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CompositeKeyWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

 		FileSystem fs = FileSystem.get(conf);
 		fs.delete(new Path(args[1]), true);
 		
 		boolean success = job.waitForCompletion(true);
 		System.out.println(success);
    }
}
