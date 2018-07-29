package com.test.MRunit;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.mapreduce.maxtemprature.MaxTemperatureMapper;
import com.mapreduce.maxtemprature.MaxTemperatureReducer;

public class MaxTemMRUnit {

	Mapper mapper;
	MapDriver mapDriver;
	Reducer reducer;
	ReduceDriver reduceDriver;
	
	@Before
	public void init() {		
		mapper = new MaxTemperatureMapper();
		mapDriver = new MapDriver(mapper);
		reducer = new MaxTemperatureReducer();
		reduceDriver = new ReduceDriver(reducer);
	}
	
	@Test
	public void testMap() throws IOException {
		Text text = new Text("0096007026999992017062218244+00000+000000FM-15+702699999V0209999C000019999999N999999999+03501+01801999999ADDMA1101731999999REMMET069MOBOB0 METAR 7026 //008 000000 221824Z AUTO 00000KT //// 34/18 A3004=");
		mapDriver.withInput(new LongWritable(1), text)
			.withOutput(new Text("2017"), new IntWritable(350))
			.runTest();
	}
	
	@Test
	public void testReduce() throws IOException{
		Text year = new Text("2017");
		ArrayList values = new ArrayList();
		values.add(new IntWritable(340));
		values.add(new IntWritable(234));
		values.add(new IntWritable(120));
		reduceDriver.withInput(year, values).withOutput(year,new IntWritable(340)).runTest();;
	}
	
}
