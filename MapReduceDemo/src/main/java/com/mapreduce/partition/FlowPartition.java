package com.mapreduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartition extends Partitioner<Text, LongWritable> {

	/**
	 * key: 手机号
	 * value: 1
	 * 
	 * 134			0
	 * 135			1
	 * 136			2
	 * 137			3
	 * 138			4
	 * others	5
	 */
	@Override
	public int getPartition(Text key, LongWritable value, int reduceNum) {
		
		String mobile = key.toString();
		
		if (mobile.startsWith("134"))
			return 0;
		else if (mobile.startsWith("135"))
			return 1;
		else if (mobile.startsWith("136"))
			return 2;
		else if (mobile.startsWith("137"))
			return 3;
		else if (mobile.startsWith("138"))
			return 4;
		else
			return 5;
	}

}
