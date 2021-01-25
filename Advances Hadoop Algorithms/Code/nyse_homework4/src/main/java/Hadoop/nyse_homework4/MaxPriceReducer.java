package Hadoop.nyse_homework4;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxPriceReducer extends Reducer<Text,CompositeKeyWritable,Text,DoubleWritable>{
	
	public void reduce(Text key, Iterable<CompositeKeyWritable> values, 
			Reducer<Text,CompositeKeyWritable,Text,DoubleWritable>.Context context) throws IOException, InterruptedException{

	Double max = 0.0;
	long max_volume = Long.MIN_VALUE;
	long min_volume = Long.MAX_VALUE;
	
	String min_date = "";
	String max_date = "";
	
	for (CompositeKeyWritable val:values) {
		if (val.getStock_price_adj_close() > max) {
			max = val.getStock_price_adj_close();
		}
		if(val.getStock_volume()>max_volume) {
			max_date = val.getMax_stock_date();
			max_volume = val.getStock_volume();
		} 
		if (val.getStock_volume() < min_volume)
		{	
			min_volume = val.getStock_volume();
			min_date = val.getMin_stock_date();
			
		}
		
	}
	
	String obj = key.toString()+"\t"+ min_volume +"\t"+ min_date +"\t"+ max_volume + "\t"+ max_date;
	context.write(new Text(obj),new DoubleWritable(max));
	}
}
