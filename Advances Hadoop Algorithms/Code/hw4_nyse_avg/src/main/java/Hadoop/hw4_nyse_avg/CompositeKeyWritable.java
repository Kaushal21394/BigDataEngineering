package Hadoop.hw4_nyse_avg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyWritable implements WritableComparable{
	
	Double local_avg;
	long count;
	
	public CompositeKeyWritable() {
		
	}
	
	public CompositeKeyWritable(Double local_avg, long count) {
		super();
		this.local_avg = local_avg;
		this.count = count;
	}
	
	public Double getLocal_avg() {
		return local_avg;
	}
	
	public void setLocal_avg(Double local_avg) {
		this.local_avg = local_avg;
	}
	
	public long getCount() {
		return count;
	}
	
	public void setCount(long count) {
		this.count = count;
	}
	
	public void readFields(DataInput in) throws IOException {
		local_avg = in.readDouble();	
		count = in.readLong();
		}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(count);
		out.writeDouble(local_avg);
		
	}
	
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		return count + "\t" + local_avg ;
	}	
	
}
