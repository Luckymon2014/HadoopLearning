package com.mapreduce.partition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCombiner extends Reducer<Text, LongWritable, Text, LongWritable>{

	LongWritable outputValue = new LongWritable();
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

		// 注意：该Reducer是在map端的，数据处理完的结构应该与map一致，送给partition处理
		// 代码和Reducer一模一样，只是在map端提前处理了一下，可以有所修改，在map端提前做一些特殊处理
		// 提前处理，可以减少一些网络传输
		
		long sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		
		outputValue.set(sum);
		context.write(key, outputValue);
				
	}
	
}
