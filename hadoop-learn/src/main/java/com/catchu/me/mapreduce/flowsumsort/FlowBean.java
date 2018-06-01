 package com.catchu.me.mapreduce.flowsumsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean >{
	
	private Long upFlow;
	
	private Long downFlow;
	
	private Long sumFlow;
	
	public FlowBean() {
	}

	public FlowBean(Long upFlow, Long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = this.upFlow+this.downFlow; 
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public int compareTo(FlowBean o) {
		return this.sumFlow>o.getSumFlow()?-1:1;
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

	public Long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	
}
