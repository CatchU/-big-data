package com.catchu.me.mapreduce.wccount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		 int total = 0;
		 for(IntWritable value : values){
			 int i = value.get();
			 total+=i;
		 }
		context.write(key, new IntWritable(total));
	}
	

	
}
