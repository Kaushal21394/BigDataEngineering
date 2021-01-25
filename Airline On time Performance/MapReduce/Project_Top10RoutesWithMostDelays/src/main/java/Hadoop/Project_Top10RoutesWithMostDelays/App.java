package Hadoop.Project_Top10RoutesWithMostDelays;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
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
 		Job job1 = Job.getInstance(config,"Routes with Most Delays");
 		
 		job1.setJarByClass(App.class);

 		job1.setMapOutputKeyClass(Text.class);
 		job1.setMapOutputValueClass(DoubleWritable.class);
 		
 		job1.setMapperClass(RouteMapper.class);
 		job1.setCombinerClass(RouteDelayReducer.class);
 		job1.setReducerClass(RouteDelayReducer.class);
 		
 		job1.setInputFormatClass(TextInputFormat.class);
 		job1.setOutputFormatClass(TextOutputFormat.class);
 		
 		job1.setOutputKeyClass(Text.class);
 		job1.setOutputValueClass(DoubleWritable.class);
 		
 		FileInputFormat.addInputPath(job1,new Path(args[0]));
 		FileOutputFormat.setOutputPath(job1,new Path(args[1]));

 		FileSystem fs = FileSystem.get(config);
 		fs.delete(new Path(args[1]), true);
 		
 		boolean success_1 = job1.waitForCompletion(true);
 		System.out.println(success_1);
 		
 		Job job2 = Job.getInstance(config,"Routes with Most Delays");
 		
 		job2.setJarByClass(App.class);

 		job2.setMapOutputKeyClass(Text.class);
 		job2.setMapOutputValueClass(DoubleWritable.class);
 		
 		job2.setMapperClass(Top10RouteMapper.class);
 		job2.setReducerClass(Top10RoutesReducer.class);
 		//job2.setNumReduceTasks(1);
 		
 		job2.setInputFormatClass(TextInputFormat.class);
 		job2.setOutputFormatClass(TextOutputFormat.class);
 		
 		job2.setOutputKeyClass(Text.class);
 		job2.setOutputValueClass(DoubleWritable.class);
 		
 		FileInputFormat.addInputPath(job2,new Path(args[1]));
 		FileOutputFormat.setOutputPath(job2,new Path(args[2]));

 		fs.delete(new Path(args[2]), true);
 		
 		boolean success_2 = job2.waitForCompletion(true);
 		System.out.println(success_2);
 		
        } catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}

    }
}
