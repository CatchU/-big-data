package com.catchu.me.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	private Long upFlow;
	
	private Long downFlow;
	
	private Long totalFlow;

	public FlowBean(){}
	
	public FlowBean(Long upFlow, Long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalFlow = upFlow+downFlow;
	}

	public Long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(Long upFlow) {
		this.upFlow = upFlow;
	}

	public Long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(Long downFlow) {
		this.downFlow = downFlow;
	}

	public Long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(Long totalFlow) {
		this.totalFlow = totalFlow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(totalFlow);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		 upFlow = in.readLong();
		 downFlow = in.readLong();
		 totalFlow = in.readLong();
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + totalFlow;
	}
	
	
	
}
