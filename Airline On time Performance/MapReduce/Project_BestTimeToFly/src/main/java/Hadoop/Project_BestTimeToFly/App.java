package Hadoop.Project_BestTimeToFly;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Weekly Partitioning");
        job1.setJarByClass(App.class);
        
        job1.setMapperClass(WeeklyTrafficMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(CustomWritable.class);
        
        job1.setPartitionerClass(TrafficPartitioner.class);
        job1.setNumReduceTasks(7);
        
        job1.setCombinerClass(TrafficReducer.class);
        job1.setReducerClass(TrafficReducer.class);    
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(CustomWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        FileSystem fs = FileSystem.get(conf);
 		fs.delete(new Path(args[1]), true);
 		
 		boolean success_1 = job1.waitForCompletion(true);
 		System.out.println(success_1);
 		
 		Job job2 = Job.getInstance(conf, "Monthly Partitioning");
 		job2.setJarByClass(App.class);
        
 		job2.setMapperClass(MonthlyTrafficMapper.class);
 		job2.setMapOutputKeyClass(Text.class);
 		job2.setMapOutputValueClass(CustomWritable.class);
        
 		job2.setPartitionerClass(TrafficPartitioner.class);
 		job2.setNumReduceTasks(12);
        
 		job2.setCombinerClass(TrafficReducer.class);
 		job2.setReducerClass(TrafficReducer.class);    
        
 		job2.setOutputKeyClass(Text.class);
 		job2.setOutputValueClass(CustomWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
 		fs.delete(new Path(args[2]), true);
 		
 		boolean success_2 = job2.waitForCompletion(true);
 		System.out.println(success_2);
 		Job job3 = Job.getInstance(conf, "Daily Partitioning");
 		job3.setJarByClass(App.class);
        
 		job3.setMapperClass(DailyTrafficMapper.class);
 		job3.setMapOutputKeyClass(Text.class);
 		job3.setMapOutputValueClass(CustomWritable.class);
        
 		job3.setPartitionerClass(TrafficPartitioner.class);
 		job3.setNumReduceTasks(31);
        
 		job3.setCombinerClass(TrafficReducer.class);
 		job3.setReducerClass(TrafficReducer.class);    
        
 		job3.setOutputKeyClass(Text.class);
 		job3.setOutputValueClass(CustomWritable.class);
        
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        
 		fs.delete(new Path(args[3]), true);
 		
 		boolean success_3 = job3.waitForCompletion(true);
 		System.out.println(success_3);
 		
 		Job job4 = Job.getInstance(conf, "Yearly Partitioning");
 		job4.setJarByClass(App.class);
        
 		job4.setMapperClass(YearlyTrafficMapper.class);
 		job4.setMapOutputKeyClass(Text.class);
 		job4.setMapOutputValueClass(CustomWritable.class);
        
 		job4.setPartitionerClass(TrafficPartitioner.class);
 		YearPartitioner.setMinLastAccessDateYear(job4, 2000);
 		job4.setNumReduceTasks(9);
 		
 		job4.setCombinerClass(TrafficReducer.class);
 		job4.setReducerClass(TrafficReducer.class);    
        
 		job4.setOutputKeyClass(Text.class);
 		job4.setOutputValueClass(CustomWritable.class);
        
        FileInputFormat.addInputPath(job4, new Path(args[0]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));
        
 		fs.delete(new Path(args[4]), true);
 		
 		boolean success_4 = job4.waitForCompletion(true);
 		System.out.println(success_4);
 		
    }
}
