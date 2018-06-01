package com.catchu.me.hadoop;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class TestHDFSClient {
	FileSystem fs = null;
	
	/**
	 * 初始化
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		 fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "root");
	}
	
	/**
	 * 测试上传
	 * @throws Exception
	 */
	@Test
	public void testUpload() throws Exception{
		fs.copyFromLocalFile(new Path("D:/test.log"), new Path("/test.log.copy"));
		fs.close();
	}
	
	@Test
	public void testDownload() throws Exception{
		fs.copyToLocalFile(new Path("/test.log.copy"), new Path("D:/"));
		fs.close();
	}
	
	/**
	 *  测试文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testMkdir() throws Exception{
		//创建
		fs.mkdirs(new Path("/aaa/bbb"));
		
		fs.delete(new Path("/aaa"), true);
		
		fs.rename(new Path("/test.log.copy"), new Path("/test.log"));
	}
	
	@Test
	public void testAllFile() throws Exception{
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()){
			System.out.println("-----------------------");
			LocatedFileStatus file = listFiles.next();
			System.out.println(file.getPath().getName());
			System.out.println(file.getAccessTime());
			System.out.println(file.getOwner());
			System.out.println(file.getReplication());
			BlockLocation[] blockLocations = file.getBlockLocations();
			for(BlockLocation bl:blockLocations){
				System.out.println(bl.getLength());
			}
		}
	}
	
	@Test
	public void testFile() throws Exception{
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		
		for(FileStatus fs:listStatus){
			if(fs.isFile()){
				System.out.println("文件名："+fs.getPath().getName());
			}
			
			if(fs.isDir()){
				System.out.println("目录名："+fs.getPath().getName());
			}
		}
	}
}
