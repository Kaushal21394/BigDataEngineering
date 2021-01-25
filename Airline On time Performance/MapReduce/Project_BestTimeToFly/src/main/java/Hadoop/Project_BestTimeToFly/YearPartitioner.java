package Hadoop.Project_BestTimeToFly;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<Text, CustomWritable> implements Configurable{

	private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
	private Configuration conf = null;
	private int minLastAccessDateYear = 0;
	
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
		minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
	}
	
	@Override
	public int getPartition(Text key, CustomWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		return Integer.parseInt(key.toString().split("\t")[0]) - minLastAccessDateYear;
	}
	
	public static void setMinLastAccessDateYear(Job job,int minLastAccessDateYear) {
		job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,minLastAccessDateYear);
	}
}
