package com.catchu.me.mapreduce.flowsumsort;

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

public class FlowBeanSecondStep {

	static class FlowBeanSecondStepMapper extends Mapper<LongWritable, Text, FlowBean, Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			String phone = split[0];
			Long upFlow = Long.parseLong(split[1]);
			Long downFlow = Long.parseLong(split[2]);
			context.write(new FlowBean(upFlow, downFlow), new Text(phone));
		}
	}
	
	static class FlowBeanSecondReducer extends Reducer<FlowBean, Text, Text, FlowBean>{

		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(values.iterator().next(), bean);
		}		
	}
	
	public static void main(String[] args) throws Exception{
		if(args==null || args.length==0){
			args = new String[2];
			args[0] = "C:/test/flowsumsortsecond/input/first.log";
			args[1] = "C:/test/flowsumsortsecond/output";
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJar("C:/test/flowsumsort/flowFirst.jar");
		job.setMapperClass(FlowBeanSecondStepMapper.class);
		job.setReducerClass(FlowBeanSecondReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
