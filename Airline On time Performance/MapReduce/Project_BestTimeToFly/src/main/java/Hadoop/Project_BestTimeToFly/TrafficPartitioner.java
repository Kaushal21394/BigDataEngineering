package Hadoop.Project_BestTimeToFly;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TrafficPartitioner extends Partitioner<Text, CustomWritable>{

	@Override
	public int getPartition(Text key, CustomWritable value, int numOfPartitions) {
		
		int val = Integer.parseInt(key.toString().split("\t")[0]);
		return (val % numOfPartitions);
	}

}
