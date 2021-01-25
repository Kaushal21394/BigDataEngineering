package Hadoop.Project_CancellationAnalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;


public class FlightCustomWritable implements WritableComparable{
	
	private String year;
	private String carrier;
	private String cancellationCode;
	public FlightCustomWritable() {
		year = "";
		carrier = "";
		cancellationCode = "";
	}
	
	public FlightCustomWritable(String year, String carrier,String cancellationCode) {
		super();
		this.year = year;
		this.carrier = carrier;
		this.cancellationCode = cancellationCode;
	}
	

	public String getCancellationCode() {
		return cancellationCode;
	}

	public void setCancellationCode(String cancellationCode) {
		this.cancellationCode = cancellationCode;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getCarrier() {
		return carrier;
	}

	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readUTF();
		carrier = in.readUTF();
		cancellationCode = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeUTF(carrier);
		out.writeUTF(cancellationCode);
	}

	public int compareTo(Object o) {
		FlightCustomWritable ck = (FlightCustomWritable)o;
		String thisValue = this.getYear();
		String otherValue = ck.getYear();
		int result = thisValue.compareTo(otherValue);
		return (result < 0 ? -1 :(result == 0 ? 0 : 1));
	}

	@Override
	public String toString() {
		return String.join("\t", Arrays.asList(year,carrier,cancellationCode));
	}
	
}
