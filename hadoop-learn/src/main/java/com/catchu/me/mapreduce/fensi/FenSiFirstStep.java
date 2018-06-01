package com.catchu.me.mapreduce.fensi;

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

//A:B,C,D,F,E,O
//B:A,C,E,K
public class FenSiFirstStep {

	static class FensiFirstStepMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(":");
			String person = split[0];
			String fans = split[1];
			String[] fansArray = fans.split(",");
			for(String fan : fansArray){
				//输出<粉丝，人>
				context.write(new Text(fan), new Text(person));
			}
		}
		
	}
	
	static class FensiFirstStepReducer extends Reducer<Text, Text, Text, Text>{

		//<粉丝，人>
		@Override
		protected void reduce(Text fan, Iterable<Text> persons, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text person : persons){
				sb.append(person).append("-");
			}
			//输出<粉丝,人-人-人-人>
			context.write(new Text(fan), new Text(sb.toString()));
		}
		
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(FenSiFirstStep.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(FensiFirstStepMapper.class);
		job.setReducerClass(FensiFirstStepReducer.class);

		FileInputFormat.setInputPaths(job, new Path("C:/test/fensi/input/test.txt"));
		FileOutputFormat.setOutputPath(job, new Path("C:/test/fensi/output"));

		job.waitForCompletion(true);

	}
}
