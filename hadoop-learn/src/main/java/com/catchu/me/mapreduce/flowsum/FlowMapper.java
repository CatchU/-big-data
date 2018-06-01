package com.catchu.me.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split("\t");
		
		String phoneNum = split[1];
		String upFlow = split[split.length-3];
		String downFlow = split[split.length-2];
		
		context.write(new Text(phoneNum), new FlowBean(Long.parseLong(upFlow),Long.parseLong(downFlow)));
	}
	
	

}
