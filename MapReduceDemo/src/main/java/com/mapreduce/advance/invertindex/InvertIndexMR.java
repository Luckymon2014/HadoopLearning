package com.mapreduce.advance.invertindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertIndexMR {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InvertIndexMR.class);
		job.setMapperClass(InvertIndexMapper.class);
		job.setReducerClass(InvertIndexReducer.class);
		
		job.setMapOutputKeyClass(InvertIndex.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(InvertIndex.class);
		job.setOutputValueClass(NullWritable.class);
		

		FileInputFormat.setInputPaths(job, new Path("/data1/invertindex"));
		FileOutputFormat.setOutputPath(job, new Path("/output/data1/invertindex"));
		
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
	
	public static class InvertIndexMapper extends Mapper<LongWritable, Text, InvertIndex, NullWritable> {
		
		String fileName = "";
		Integer lineNum = 0;
		
		/**
		 * 在map启动时，执行setup方法（只执行一次）
		 * 获取文件名
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, InvertIndex, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			super.setup(context);
			
			InputSplit split = context.getInputSplit();
			FileSplit file = (FileSplit) split;
			
			fileName = file.getPath().getName();
			
		}

		/**
		 * <word,filename,lineNum,1>
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, InvertIndex, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String values[] = line.split(" ");
			
			for (String word : values) {
				InvertIndex index = new InvertIndex(word, fileName, lineNum, 1);
				context.write(index, NullWritable.get());
			}
			
			lineNum++;
			
		}
		
	}
	
	public static class InvertIndexReducer extends Reducer<InvertIndex, NullWritable, InvertIndex, NullWritable> {

		@Override
		protected void reduce(InvertIndex key, Iterable<NullWritable> values,
				Reducer<InvertIndex, NullWritable, InvertIndex, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			Integer count = 0;
			
			for (NullWritable value : values) {
				count++;
			}
			
			InvertIndex index = new InvertIndex(key.getWord(), key.getFile(), key.getLineNum(), 0);
			index.setTimes(count);
			
			context.write(index, NullWritable.get());
			
		}
		
	}

}
