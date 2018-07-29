package com.mapreduce.advance.secondsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondSortMapReduce {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SecondSortMapReduce.class);
		job.setMapperClass(SecondSortMapper.class);
		job.setReducerClass(SecondSortReducer.class);
		
		job.setMapOutputKeyClass(NumberSort.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(NumberSort.class);
		job.setOutputValueClass(NullWritable.class);
		

		FileInputFormat.setInputPaths(job, new Path("/data1/num.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/output/data1/num"));
		
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
	
	public static class SecondSortMapper extends Mapper<LongWritable, Text, NumberSort, NullWritable> {
		
		/**
		 * input: 20 20
		 * output: <20,20> <null>
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NumberSort, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] values = line.split(" ");
			long first = Long.parseLong(values[0]);
			long second = Long.parseLong(values[1]);
			
			NumberSort ns = new NumberSort(first, second);
			
			context.write(ns, NullWritable.get());
			
		}
		
	}
	
	public static class SecondSortReducer extends Reducer<NumberSort, NullWritable, NumberSort, NullWritable> {

		@Override
		protected void reduce(NumberSort key, Iterable<NullWritable> values,
				Reducer<NumberSort, NullWritable, NumberSort, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
			
		}
		
	}

}
