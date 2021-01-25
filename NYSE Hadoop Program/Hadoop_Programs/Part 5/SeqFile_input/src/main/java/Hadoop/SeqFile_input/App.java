package Hadoop.SeqFile_input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {

    	Configuration config = new Configuration();
    	try {
			Job job = Job.getInstance(config,"Sequence Count");
			job.setJarByClass(App.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setInputFormatClass(SequenceFileInputFormat.class);
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
