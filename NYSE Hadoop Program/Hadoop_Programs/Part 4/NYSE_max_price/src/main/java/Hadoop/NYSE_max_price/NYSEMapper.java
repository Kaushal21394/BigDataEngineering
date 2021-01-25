package Hadoop.NYSE_max_price;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class NYSEMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	DoubleWritable max_value = new DoubleWritable(0.0);
	Text symbol = new Text();
	public void map(LongWritable key,Text value, Context context) {
		try {
            if (key.get() == 0 && value.toString().contains("header") )
                return;
            else {
            	String line = value.toString();
        		String [] tokens = line.split(",");
        		symbol.set(tokens[1]);
        		max_value.set(Double.valueOf(tokens[4]));
        		context.write(symbol, max_value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }		
	}
}
