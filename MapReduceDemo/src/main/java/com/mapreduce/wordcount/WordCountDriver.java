package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jcraft.jsch.Compression;

//Input
//Deer Bear River Car Car River Deer Car Bear

//Splitting
//Deer Bear River
//Car Car River
//Deer Car Bear

//Mapping
//Deer 1; Bear 1; River 1
//Car 1; Car 1; River 1
//Deer 1; Car 1; Bear 1

//Shuffling
//Bear 1; Bear 1
//Car 1; Car 1; Car 1
//Deer 1; Deer 1
//River 1; River 1

//Reducing
//Bear 2
//Car 3
//Deer 2
//River 2

//Result
//Bear 2; Car 3; Deer 2; River 2
public class WordCountDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// Hadoop配置文件，配置MapReduce的上下文环境
		Configuration conf = new Configuration();
		// 指定hdfs相关的参数
		conf.set("fs.defaultFS", "hdfs://hadoop001:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		// 为作业配置一个Job
		Job job = Job.getInstance(conf);
		
		// 设置入口类
		job.setJarByClass(WordCountDriver.class);
		// 设置MapReduce
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 设置输出类
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// 如更改Map输出类型，需要设置MapOutput类，本例中可以不设置
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		// Map端开启压缩
		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, Compression.class);

		// Output开启压缩
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		// 设置输入/输出文件，注意是hadoop上的文件
		// 文件输入路径
		Path inputPath = new Path("/data1/data.txt");
		FileInputFormat.setInputPaths(job, inputPath);
		// 文件输出路径，如果路径已存在，会报错
		Path outputPath = new Path("/output/data1/data.txt");
		// 判断输出路径是否存在，存在就删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// 程序结束后正常退出
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}

}
