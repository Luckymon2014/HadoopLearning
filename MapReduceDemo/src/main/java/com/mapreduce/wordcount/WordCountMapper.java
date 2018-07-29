package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	// 输出对象放在外面，节省空间
	Text outputKey = new Text();
	LongWritable outputValue = new LongWritable(1L); // 每个单词读到即计数1

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {

		// 切分输入文件（用空格切分）
		String line = value.toString();
		String[] values = line.split(" ");

		for (String word : values) {
			outputKey.set(word);
			context.write(outputKey, outputValue);
		}

	}

}
