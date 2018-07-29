package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	LongWritable outputValue = new LongWritable();
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		// 将所有单词出现次数相加
		long sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		
		outputValue.set(sum);
		context.write(key, outputValue);
		
	}
}
