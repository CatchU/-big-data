package com.catchu.me.mapreduce.fensi;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//A	I-K-C-B-G-F-H-O-D-
//<粉丝,人-人-人-人>
public class FenSiSecondStep {

	static class FensiSecondStepMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			String fan = split[0];
			String[] persons = split[1].split("-");
			
			Arrays.sort(persons);
			
			for(int i=0;i<persons.length-1;i++){
				for(int j=i+1;j<persons.length;j++){
					//输出<人-人，粉丝>
					context.write(new Text(persons[i]+"-"+persons[j]), new Text(fan));
				}
			}
		}
		
	}
	
	static class FensiSecondStepReducer extends Reducer<Text, Text, Text, Text>{

		//<人-人，粉丝>
		@Override
		protected void reduce(Text person_person, Iterable<Text> fans, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text fan : fans){
				sb.append(fan).append(",");
			}
			context.write(new Text(person_person),new Text(sb.toString()));
		}
		
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(FenSiSecondStep.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(FensiSecondStepMapper.class);
		job.setReducerClass(FensiSecondStepReducer.class);

		FileInputFormat.setInputPaths(job, new Path("C:/test/fensisecond/input/input.txt"));
		FileOutputFormat.setOutputPath(job, new Path("C:/test/fensisecond/output"));

		job.waitForCompletion(true);

	}
}
