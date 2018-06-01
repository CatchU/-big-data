package com.catchu.me.mapreduce.wccount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		if(args==null || args.length==0){
			args = new String[2];
			args[0] = "C:/test/wccount/input/input.txt";
			args[1] = "C:/test/wccount/output/";
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJar("C:/test/wccount/wc.jar");
//		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean flag = job.waitForCompletion(true);
		System.exit(flag?0:1);

	}

}
