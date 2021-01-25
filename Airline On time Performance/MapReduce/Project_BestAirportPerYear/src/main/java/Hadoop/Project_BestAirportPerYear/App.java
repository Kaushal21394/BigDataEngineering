package Hadoop.Project_BestAirportPerYear;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, InterruptedException
    {
Configuration config = new Configuration();
        
        try {
 		Job job1 = Job.getInstance(config,"for Each Airport : TotalDelay/Total Trips Partitioned by Year");
 		
 		job1.setJarByClass(App.class);

 		job1.setMapOutputKeyClass(Text.class);
 		job1.setMapOutputValueClass(FlightAnalysisCustomWritable.class);
 		
 		job1.setMapperClass(CountMapper.class);
 		job1.setReducerClass(BestMetricReducer.class);
 		
 		job1.setInputFormatClass(TextInputFormat.class);
 		job1.setOutputFormatClass(TextOutputFormat.class);
 		
 		job1.setOutputKeyClass(Text.class);
 		job1.setOutputValueClass(FloatWritable.class);
 		
 		FileInputFormat.addInputPath(job1,new Path(args[0]));
 		FileOutputFormat.setOutputPath(job1,new Path(args[1]));

 		FileSystem fs = FileSystem.get(config);
 		fs.delete(new Path(args[1]), true);
 		
 		boolean success1 = job1.waitForCompletion(true);
 		System.out.println(success1);
 		
 		Job job2 = Job.getInstance(config,"for Each Airport : TotalDelay/Total Trips Partitioned by Year");
 		
 		job2.setJarByClass(App.class);

 		//job2.setMapOutputKeyClass(Text.class);
 		//job2.setMapOutputValueClass(FlightAnalysisCustomWritable.class);
 		
 		job2.setMapperClass(BestMetricMapper.class);
 		job2.setMapperClass(AirportMapper.class);
 		job2.setReducerClass(JoinReducer.class);
 		//job2.setNumReduceTasks(0);

        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, BestMetricMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, AirportMapper.class);
        job2.getConfiguration().set("join.type", "inner");
 		
 		job2.setOutputKeyClass(Text.class);
 		job2.setOutputValueClass(Text.class);
 		
 		FileInputFormat.addInputPath(job2,new Path(args[1]));
 		FileOutputFormat.setOutputPath(job2,new Path(args[3]));

 		
 		//fs.delete(new Path(args[2]), true);
 		fs.delete(new Path(args[3]), true);
 		
 		boolean success2 = job2.waitForCompletion(true);
 		System.out.println(success2);
 		
 		
 		Job job3 = Job.getInstance(config,"Top 10 Airports Partitioned by Year");
 		
 		job3.setJarByClass(App.class);

 		job3.setMapOutputKeyClass(Text.class);
 		job3.setMapOutputValueClass(FloatWritable.class);
 		
 		job3.setMapperClass(Top10AirportsPerYearMapper.class);
 		job3.setReducerClass(Top10AirportsReducer.class);

 		job3.setPartitionerClass(YearPartitioner.class);
 		YearPartitioner.setMinLastAccessDateYear(job3, 2000);
 		job3.setNumReduceTasks(9);
 		
 		//job3.setPartitionerClass(NaturalKeyPartitioner.class);;
		//job3.setGroupingComparatorClass(NaturalKeyGroupComparator.class);
		//job3.setSortComparatorClass(SecondaryGroupComparator.class);
		
 		
 		job3.setInputFormatClass(TextInputFormat.class);
 		job3.setOutputFormatClass(TextOutputFormat.class);
 		
 		job3.setOutputKeyClass(Text.class);
 		job3.setOutputValueClass(FloatWritable.class);
 		
 		FileInputFormat.addInputPath(job3,new Path(args[3]));
 		FileOutputFormat.setOutputPath(job3,new Path(args[4]));

 		fs.delete(new Path(args[4]), true);
 		
 		boolean success3 = job3.waitForCompletion(true);
 		System.out.println(success3);

 		
 		
        } catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}

    }
}
