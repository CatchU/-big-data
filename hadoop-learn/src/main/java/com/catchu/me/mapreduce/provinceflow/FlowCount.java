package com.catchu.me.mapreduce.provinceflow;

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

public class FlowCount {

	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String phone = fields[0];
			String upFlow = fields[fields.length-2];
			String downFlow = fields[fields.length-1];
			
			context.write(new Text(phone), new FlowBean(Long.parseLong(upFlow), Long.parseLong(downFlow)));
		}	
	}
	
	
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

		@Override
		protected void reduce(Text phone, Iterable<FlowBean> beans, Context context)throws IOException, InterruptedException {
			long upFlow = 0;
			long downFlow = 0;
			//遍历beans,获取上下行流量并累加
			for(FlowBean flowBean : beans){
				upFlow += flowBean.getUpFlow();
				downFlow += flowBean.getDownFlow();
			}
			
			context.write(new Text(phone), new FlowBean(upFlow, downFlow));
		}
	} 
	
	public static void main(String[] args) throws Exception {
		
		if(args==null || args.length==0){
			args = new String[2];
			args[0]="C:/test/proviceflow/input/flow.log";
			args[1]="C:/test/proviceflow/output";
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJar("C:/test/proviceflow/flow.jar");
		//job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setPartitionerClass(FlowPartitioner.class);
		job.setNumReduceTasks(1);   //在本地运行，只有1台机器，如果是放在服务器集群中可以写多个，比如5
 		
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
