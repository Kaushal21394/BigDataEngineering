package Hadoop.Gender;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, InterruptedException
    {
    	Configuration config = new Configuration();
    	config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
    	try {
			Job job = Job.getInstance(config,"Gender Count");
			job.setJarByClass(App.class);
			
			job.setMapperClass(GenderMapper.class);
			job.setReducerClass(GenderReducer.class);
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);
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
