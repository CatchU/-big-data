package com.catchu.me.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		for(FlowBean fb:values){
			sum_upFlow += fb.getUpFlow();
			sum_downFlow += fb.getDownFlow();
		}
		
		FlowBean flowBean = new FlowBean(sum_upFlow, sum_downFlow);
		context.write(key, flowBean);
	}
	

}
