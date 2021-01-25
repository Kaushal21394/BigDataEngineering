package Hadoop.NYSE_avg_stock_price;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	
	DoubleWritable price = new DoubleWritable(0.0);
	Text symbol = new Text();
	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		try {
            if (key.get() == 0 && value.toString().contains("header") )
                return;
            else {
        
		String line = value.toString();
		String[] tokens = line.split(",");
		
		symbol.set(tokens[1]);
		price.set(Double.valueOf(tokens[4]));
		
		context.write(symbol,price);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }	
	}

}
