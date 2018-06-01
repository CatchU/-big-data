package com.catchu.me.hadoop;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * 使用流的方式操作
 * @author CatchU
 *
 */
public class TestHDFSStream {

	FileSystem fs = null;
	
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://mini1:9000");
		fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "root");
	}
	
	/**
	 * 测试上传
	 * @throws Exception
	 */
	@Test
	public void testUpload() throws Exception{
		FSDataOutputStream outputStream = fs.create(new Path("/test"), true);
		
		FileInputStream inputStream = new FileInputStream("d:/test.log");
		int copy = IOUtils.copy(inputStream, outputStream);
	}
	
	/**
	 * 测试下载
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testDownload() throws Exception{
		FSDataInputStream inputStream = fs.open(new Path("/test"));
		
		FileOutputStream outputStream = new FileOutputStream("d:/haha.txt"); 
		IOUtils.copy(inputStream, outputStream);
	}
	
	/**
	 * 输出到控制台
	 * @throws Exception
	 */
	@Test
	public void show() throws Exception{
		FSDataInputStream inputStream = fs.open(new Path("/canglaoshi.avi"));
		
		org.apache.hadoop.io.IOUtils.copyBytes(inputStream,System.out,1024);
	}
}
