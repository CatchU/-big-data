package com.catchu.me.mapreduce.rjoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
* 订单表和商品表合到一起
order.txt(订单id, 日期, 商品编号, 数量)
	1001	20150710	P0001	2
	1002	20150710	P0001	3
	1002	20150710	P0002	3
	1003	20150710	P0003	3
product.txt(商品编号, 商品名字, 价格, 数量)
	P0001	小米5	1001	2
	P0002	锤子T1	1000	3
	P0003	锤子	1002	4
 */
public class RJoin {

	static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			InfoBean bean = new InfoBean();
			
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String name = inputSplit.getPath().getName();
			String pid = "";
			if(name.startsWith("order")){
				String[] split = line.split("\t");
				pid = split[2];
				//订单信息
				bean.set(Integer.parseInt(split[0]), split[1],split[2],Integer.parseInt(split[3]),"",0,0,"0");		
			}else{
				//商品信息
				String[] split = line.split("\t");
				pid = split[0];
				bean.set(0, "", pid, 0, split[1], Integer.parseInt(split[2]), Float.parseFloat(split[3]), "1");
			}
			
			context.write(new Text(pid), bean);
		}
		
	}
	
	
	static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{

		@Override
		protected void reduce(Text text, Iterable<InfoBean> beans,Context context) throws IOException, InterruptedException {
			
			ArrayList<InfoBean> orBeanList = new ArrayList<InfoBean>();
			InfoBean pdBean = new InfoBean();
			for(InfoBean bean:beans){
				if("0".equals(bean.getFlag())){
					//订单
					InfoBean orderBean = new InfoBean();
					try {
						BeanUtils.copyProperties(orderBean, bean);
						orBeanList.add(orderBean);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else{
					//商品
					try {
						BeanUtils.copyProperties(pdBean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			//遍历拼接
			for(InfoBean bean : orBeanList){
				bean.setPname(pdBean.getPname());
				bean.setCategory_id(pdBean.getCategory_id());
				bean.setPrice(pdBean.getPrice());
				
				context.write(bean, NullWritable.get());
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		if(args==null || args.length==0){
			args = new String[2];
			args[0] = "C:/test/rjoin/input/";
			args[1] = "C:/test/rjoin/output/";
		}
		Configuration conf = new Configuration();
		
		conf.set("mapred.textoutputformat.separator", "\t");
		
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		 //job.setJarByClass(RJoin.class);
		job.setJar("C:/test/rjoin/join.jar");

		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(RJoinMapper.class);
		job.setReducerClass(RJoinReducer.class);

		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
	
}
