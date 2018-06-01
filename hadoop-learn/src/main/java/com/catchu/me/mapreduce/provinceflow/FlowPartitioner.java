package com.catchu.me.mapreduce.provinceflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 定义partitioner，分发至不同的reducetask
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean>{

	private static Map<String,Integer> proviceDirc = new HashMap<String,Integer>();
	
	static{
		proviceDirc.put("136", 0);
		proviceDirc.put("137", 1);
		proviceDirc.put("138", 2);
		proviceDirc.put("139", 3);
	}
	
	@Override
	public int getPartition(Text phone, FlowBean bean, int numPartitions) {
		String prefix = phone.toString().substring(0, 3);
		Integer code = proviceDirc.get(prefix);
		return code==null?5:code;
	}

}
