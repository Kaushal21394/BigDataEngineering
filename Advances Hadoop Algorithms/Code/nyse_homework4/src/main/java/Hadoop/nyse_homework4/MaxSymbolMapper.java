package Hadoop.nyse_homework4;

import java.io.IOException;
import java.sql.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxSymbolMapper extends Mapper<LongWritable,Text,Text,CompositeKeyWritable> {
	
	public void map(LongWritable key , Text value, Mapper<LongWritable,Text,Text,CompositeKeyWritable>.Context context ) throws IOException, InterruptedException {
		
	try {
		if (key.get() == 0 && value.toString().contains("header") )
            return;
        else {
        
		String line = value.toString();
		String[] tokens = line.split(",");
		
		String stock_volume = tokens[7];
		String max_price = tokens[8];
		String date = tokens[2];
		
		CompositeKeyWritable object = new CompositeKeyWritable(Long.parseLong(stock_volume),Double.valueOf(max_price),date,date);
		
		context.write(new Text(tokens[1]),object);
        }
	}
	catch (Exception e) {
        e.printStackTrace();
    }
		
	}
	
	
}
