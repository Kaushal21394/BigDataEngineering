package Hadoop.nyse_homework4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;


public class CompositeKeyWritable implements WritableComparable {
	
	long stock_volume;
	Double stock_price_adj_close;
	String min_stock_date;
	String max_stock_date;
	
	public CompositeKeyWritable() {
		
	}
	
	public CompositeKeyWritable(long stock_volume,Double stock_price_adj_close, String min_stock_date,String max_stock_date) {
		super();
		this.stock_volume = stock_volume;
		this.stock_price_adj_close = stock_price_adj_close;
		this.min_stock_date = min_stock_date;
		this.max_stock_date = max_stock_date;
	}

	
	public long getStock_volume() {
		return stock_volume;
	}

	public void setStock_volume(long stock_volume) {
		this.stock_volume = stock_volume;
	}

	public Double getStock_price_adj_close() {
		return stock_price_adj_close;
	}

	public void setStock_price_adj_close(Double stock_price_adj_close) {
		this.stock_price_adj_close = stock_price_adj_close;
	}

	

	public String getMin_stock_date() {
		return min_stock_date;
	}

	public void setMin_stock_date(String min_stock_date) {
		this.min_stock_date = min_stock_date;
	}

	public String getMax_stock_date() {
		return max_stock_date;
	}

	public void setMax_stock_date(String max_stock_date) {
		this.max_stock_date = max_stock_date;
	}

	public void readFields(DataInput in) throws IOException {
		stock_volume = Long.parseLong(in.readUTF());
		stock_price_adj_close = Double.valueOf(in.readUTF());
		max_stock_date = in.readUTF();
		min_stock_date = in.readUTF();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(String.valueOf(stock_volume));
		out.writeUTF(stock_price_adj_close.toString());
		out.writeUTF(max_stock_date);
		out.writeUTF(min_stock_date);
	}

	public int compareTo(Object o) {
		return 0;
	}
	
	@Override
	public String toString() {
		return min_stock_date + "\t" + max_stock_date + "\t" + stock_price_adj_close + "\t" + stock_volume;
	}
	
}
