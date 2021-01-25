package Hadoop.Project_CancellationAnalysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CancellationCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
	
	public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException, InterruptedException {
		long sum = 0;

		Text outKey = new Text();
		LongWritable outValue = new LongWritable(0);
		for(LongWritable val :value) {
			sum += val.get();
		}
		
		outValue.set(sum);
		outKey.set(key);
		//outKey.set(new Text (String.join("\t", Arrays.asList(key.getYear(),key.getCarrier(),key.getCancellationCode()))));	
		
		context.write(outKey, outValue);
	}

}
