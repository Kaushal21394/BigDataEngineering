package Hadoop.Project_BestTimeToFly;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class CustomWritable implements WritableComparable{
	int arrDelay = 0;
	int taxing = 0;
	int flightCount =0;
	int cancelled = 0;
	int diverted = 0;
	
	public CustomWritable() {
	
		arrDelay = 0;
		taxing = 0;
		flightCount =0;
		cancelled = 0;
		diverted = 0;
		
	}
	
	public CustomWritable(int arrDelay, int taxing, int flightCount, int cancelled, int diverted) {
		super();
		this.arrDelay = arrDelay;
		this.taxing = taxing;
		this.flightCount = flightCount;
		this.cancelled = cancelled;
		this.diverted = diverted;
	}

	
	public int getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(int arrDelay) {
		this.arrDelay = arrDelay;
	}

	public int getTaxing() {
		return taxing;
	}

	public void setTaxing(int taxing) {
		this.taxing = taxing;
	}

	public int getFlightCount() {
		return flightCount;
	}

	public void setFlightCount(int flightCount) {
		this.flightCount = flightCount;
	}

	public int getCancelled() {
		return cancelled;
	}

	public void setCancelled(int cancelled) {
		this.cancelled = cancelled;
	}

	public int getDiverted() {
		return diverted;
	}

	public void setDiverted(int diverted) {
		this.diverted = diverted;
	}

	public void readFields(DataInput in) throws IOException {
		arrDelay = in.readInt();
		taxing = in.readInt();
		flightCount = in.readInt();
		cancelled = in.readInt();
		diverted = in.readInt();
		
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(arrDelay);
		out.writeInt(taxing);
		out.writeInt(flightCount);
		out.writeInt(cancelled);
		out.writeInt(diverted);
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return String.join("\t", Arrays.asList(String.valueOf(arrDelay),String.valueOf(taxing),String.valueOf(flightCount),String.valueOf(cancelled),String.valueOf(diverted)));
	}
	
	

}
