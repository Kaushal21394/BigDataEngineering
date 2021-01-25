package Hadoop.Project_PercentageDelayPerYear;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlightDelayCustomWritable implements WritableComparable {

	double totalCount;
	double totalDelay;
	
	public FlightDelayCustomWritable() {
		totalCount = 0.0;
		totalDelay = 0.0;
	}
	
	public FlightDelayCustomWritable(double totalCount, double totalDelay) {
		super();
		this.totalCount = totalCount;
		this.totalDelay = totalDelay;
	}
	
	public double getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(double totalCount) {
		this.totalCount = totalCount;
	}

	public double getTotalDelay() {
		return totalDelay;
	}

	public void setTotalDelay(double totalDelay) {
		this.totalDelay = totalDelay;
	}

	public void readFields(DataInput in) throws IOException {
		totalCount = in.readDouble();
		totalDelay = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(totalCount);
		out.writeDouble(totalDelay);
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return totalDelay + "\t" + totalCount;
	}
	
	
}
