package com.catchu.me.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(null==args || args.length==0){
			args = new String[2];
			args[0] = "C:/test/flowsum/input/flow.log";
			args[1] = "C:/test/flowsum/output/";
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJar("C:/test/flowsum/flowsum.jar");
		
//		job.setJarByClass(FlowDriver.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		
	}

}
