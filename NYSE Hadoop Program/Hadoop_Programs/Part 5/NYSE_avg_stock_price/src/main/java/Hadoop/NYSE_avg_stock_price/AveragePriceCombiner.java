package Hadoop.NYSE_avg_stock_price;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveragePriceCombiner extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	
	DoubleWritable result = new DoubleWritable(0.0);
	protected void reduce(Text key, Iterable<DoubleWritable> values, 
			Reducer<Text,DoubleWritable,Text,DoubleWritable>.Context context) throws IOException, InterruptedException {
	    double sum = 0.0;
	    for (DoubleWritable value : values) {
	        sum += value.get();
	    }
	    result.set(sum);
	    context.write(key, result);
	}

}
