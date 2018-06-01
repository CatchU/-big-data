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

public class FlowSumFirstStep {

	static class FlowSumFirstStepMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			String phone = split[1];
			String upFlowStr = split[split.length-3];
			String downFlowStr = split[split.length-2];
			context.write(new Text(phone), new FlowBean(Long.parseLong(upFlowStr),Long.parseLong(downFlowStr)));
		}
	}
	
	static class FlowSumFirstStepReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

		@Override
		protected void reduce(Text key, Iterable<FlowBean> value, Context context)
				throws IOException, InterruptedException {
			//<18300705180,new FlowBean(1,2)>,<18300705180,new FlowBean(5,6)>
			Long sumUpFlow = 0L;
			Long sumDownFLow = 0L;
			//遍历获取每个手机号的上下行总流量
			for(FlowBean fb : value){
				if(null!=fb){
					sumUpFlow+=fb.getUpFlow();
					sumDownFLow+=fb.getDownFlow();
				}
			}
			context.write(new Text(key), new FlowBean(sumUpFlow,sumDownFLow));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		if(args==null ||args.length==0){
			args = new String[2];
			args[0]="C:/test/flowsumsort/input/test.dat";
			args[1]="C:/test/flowsumsort/output/";
		}
		job.setJar("C:/test/flowsumsort/flowFirst.jar");
		//job.setJarByClass(FlowSumFirstStep.class);
		job.setMapperClass(FlowSumFirstStepMapper.class);
		job.setReducerClass(FlowSumFirstStepReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
	
}
