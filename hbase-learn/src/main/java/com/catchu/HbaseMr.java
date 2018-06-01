package com.catchu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 刘俊重
 * @Description 使用mapreduce操作hbase
 * 1.初始化初始表和操作之后的导出表
 * 2.创建mapper统计每个单词的个数
 * 3.创建reducer将统计之后的个数输出到hbase的表中
 * @date 11:23
 */
public class HbaseMr {
    /**
     * 表信息
     */
    private static final String TABLE_NAME = "word";
    private static final String COLUMN_FAMILY = "content";
    private static final String COLUMN = "info";
    private static final String OUT_TABLE_NAME = "stat";

    /**
     * HBase配置
     */
    static Configuration conf;
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "mini2,mini3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * 初始化表
     */
    public static void initTb(){
        HTable table=null;
        HBaseAdmin admin=null;
        try {
            admin = new HBaseAdmin(conf);
            TableName word = TableName.valueOf(TABLE_NAME);
            TableName stat = TableName.valueOf(OUT_TABLE_NAME);
            //删除表
            if(admin.tableExists(word) || admin.tableExists(stat)){
                System.out.println("表已存在，已经先删除了");
                admin.disableTable(word);
                admin.deleteTable(word);
                admin.disableTable(stat);
                admin.deleteTable(stat);
            }
            //创建表
            HTableDescriptor wordDesc = new HTableDescriptor(word);
            HColumnDescriptor colf = new HColumnDescriptor(COLUMN_FAMILY);
            wordDesc.addFamily(colf);
            admin.createTable(wordDesc);
            HTableDescriptor statDesc = new HTableDescriptor(stat);
            HColumnDescriptor colf2 = new HColumnDescriptor(COLUMN_FAMILY);
            statDesc.addFamily(colf2);
            admin.createTable(statDesc);
            //导入数据
            table = new HTable(conf,TABLE_NAME);
            table.setAutoFlush(false);
            table.setWriteBufferSize(500);
            List<Put> lp = new ArrayList<Put>();
            Put p1 = new Put(Bytes.toBytes("1"));
            p1.addColumn(COLUMN_FAMILY.getBytes(), COLUMN.getBytes(),	("The Apache Hadoop software library is a framework").getBytes());
            lp.add(p1);
            Put p2 = new Put(Bytes.toBytes("2"));
            p2.addColumn(COLUMN_FAMILY.getBytes(),COLUMN.getBytes(),("The common utilities that support the other Hadoop modules").getBytes());
            lp.add(p2);
            Put p3 = new Put(Bytes.toBytes("3"));
            p3.addColumn(COLUMN_FAMILY.getBytes(), COLUMN.getBytes(),("Hadoop by reading the documentation").getBytes());
            lp.add(p3);
            Put p4 = new Put(Bytes.toBytes("4"));
            p4.addColumn(COLUMN_FAMILY.getBytes(), COLUMN.getBytes(),("Hadoop from the release page").getBytes());
            lp.add(p4);
            Put p5 = new Put(Bytes.toBytes("5"));
            p5.addColumn(COLUMN_FAMILY.getBytes(), COLUMN.getBytes(),("Hadoop on the mailing list").getBytes());
            lp.add(p5);
            table.put(lp);
            table.flushCommits();
            lp.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(table!=null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建map类统计单词个数
     * Text 输出的key
     * IntWritable 输出的value
     */
    class MyMapper extends TableMapper<Text,IntWritable>{
        /**
         * @param key hbase中的rowkey
         * @param value rowkey对应的那条记录
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //根据value获取一个cell的值
            byte[] value1 = value.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN));
            String words = Bytes.toString(value1);
            //按空格分割
            String[] wordsArr = words.split(" ");
            //输出
            for(int i=0;i<wordsArr.length;i++){
                context.write(new Text(wordsArr[i]),new IntWritable(1));
            }
        }
    }

    /**
     * 创建reducer最终统计输出结果
     * Text 输入的key
     * IntWritable 输入的value
     * ImmutableBytesWritable 输出类型，表示rowkey的类型
      */
    class MyReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable>{
        /**
         * @param key 输入的key
         * @param values 输入的value
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //对mapper数据求和
            int sum = 0;
            for(IntWritable val: values){
                sum+=val.get();
            }
            //创建要向hbase插入的数据
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY),Bytes.toBytes(COLUMN),Bytes.toBytes(sum));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }

    public static void main(String[] args) throws Exception{
        conf.set("df.default.name", "hdfs://mini1:9000/");//设置hdfs的默认路径
        conf.set("hadoop.job.ugi", "root,root");//用户名，组
        conf.set("mapred.job.tracker", "mini1:9000");//设置jobtracker在哪
        //初始化表
        initTb();//初始化表
        //创建job
        Job job = new Job(conf, "HbaseMr");//job
        job.setJarByClass(HbaseMr.class);//主类
        //创建scan
        Scan scan = new Scan();
        //可以指定查询某一列
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN));
        //创建查询hbase的mapper，设置表名、scan、mapper类、mapper的输出key、mapper的输出value
        TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, MyMapper.class,Text.class, IntWritable.class, job);
        //创建写入hbase的reducer，指定表名、reducer类、job
        TableMapReduceUtil.initTableReducerJob(OUT_TABLE_NAME, MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
