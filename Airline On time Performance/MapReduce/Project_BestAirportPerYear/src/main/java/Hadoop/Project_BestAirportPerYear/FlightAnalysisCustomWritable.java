package Hadoop.Project_BestAirportPerYear;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlightAnalysisCustomWritable implements WritableComparable{
	
	long totalDelay;
	long totalTrips;
	
	public FlightAnalysisCustomWritable() {
		totalDelay = 0;
		totalTrips = 0;
	}
	
	public FlightAnalysisCustomWritable(long totalDelay, long totalTrips) {
		super();
		this.totalDelay = totalDelay;
		this.totalTrips = totalTrips;
	}
	
	
	
	public long getTotalDelay() {
		return totalDelay;
	}



	public void setTotalDelay(long totalDelay) {
		this.totalDelay = totalDelay;
	}



	public long getTotalTrips() {
		return totalTrips;
	}



	public void setTotalTrips(long totalTrips) {
		this.totalTrips = totalTrips;
	}



	public void readFields(DataInput in) throws IOException {
		totalDelay = in.readLong();
		totalTrips = in.readLong();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(totalDelay);
		out.writeLong(totalTrips);
		
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return totalDelay + "\t"+ totalTrips ;
	}

	
}
