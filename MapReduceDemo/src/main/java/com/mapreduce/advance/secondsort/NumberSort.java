package com.mapreduce.advance.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NumberSort implements WritableComparable<NumberSort>{
	
	// 第一个数
	private long first;
	// 第二个数
	private long second;
	
	// 构造方法
	public NumberSort(){}
	
	public NumberSort(long first, long second) {
		super();
		this.first = first;
		this.second = second;
	}
	
	/** 
	 * 复写toString方法
	 */
	@Override
	public String toString() {
		return first + "," + second;
	}

	public void readFields(DataInput in) throws IOException {
		first = in.readLong();
		second = in.readLong();		
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(first);
		out.writeLong(second);
	}

	public int compareTo(NumberSort o) {
		// 升序
		long num = this.first - o.first;
		
		if (num != 0) {
			return (int) num;
		} else {
			num = this.second - o.second;
			return (int) num;
		}
	}

}
