package Hadoop.hw4_nyse_avg;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text, IntWritable, CompositeKeyWritable> {

	IntWritable year = new IntWritable();
	CompositeKeyWritable obj = new CompositeKeyWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if (key.get() == 0 && value.toString().contains("header") )
	            return;
			else {	
				SimpleDateFormat frmt = new SimpleDateFormat("yyyy-mm-dd");
				String[] tokens = value.toString().split(",");
				String strDate = tokens[2];
				
				Date creationDate = frmt.parse(strDate);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
				Double stock_avg_close = Double.parseDouble(tokens[8]); 
				
				year.set(Integer.parseInt((dateFormat.format(creationDate))));	
				obj.setCount(1);
				obj.setLocal_avg(stock_avg_close);
				
				context.write(year, obj);
		}
    	}
    	catch (Exception e) {
            e.printStackTrace();
        }
	}
}