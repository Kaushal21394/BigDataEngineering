package Hadoop.NYSE_max_price;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NYSEmaxReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	
	
	protected void reduce(Text key, Iterable<DoubleWritable> values, 
			Reducer<Text,DoubleWritable,Text,DoubleWritable>.Context context) throws IOException, InterruptedException {
	    double max = 0.0;
	    for (DoubleWritable value : values) {
	        if (value.get() > max) {
	            max = value.get();
	        }
	    }
	    context.write(key, new DoubleWritable(max));
		
	}

}
