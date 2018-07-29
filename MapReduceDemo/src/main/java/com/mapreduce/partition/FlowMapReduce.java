package com.mapreduce.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowMapReduce {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowMapReduce.class);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// 设置Combiner
		job.setCombinerClass(FlowCombiner.class);
		
		// 设置reduce个数
		job.setNumReduceTasks(6);
		
		// 设置Partitions
		job.setPartitionerClass(FlowPartition.class);
		
		FileInputFormat.setInputPaths(job, new Path("/data1/flow.log"));
		FileOutputFormat.setOutputPath(job, new Path("/output/data1/flow"));
		
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class FlowMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		Text outputKey = new Text();
		LongWritable outputValue = new LongWritable(1L);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] values = line.split("\t");
			String mobile = values[1];
			outputKey.set(mobile);
			context.write(outputKey, outputValue);
		}
		
	}
	
	public static class FlowReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		LongWritable outputValue = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			
			outputValue.set(sum);
			context.write(key, outputValue);
			
		}
		
	}
	
	
	
}
