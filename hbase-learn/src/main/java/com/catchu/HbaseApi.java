package com.catchu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 刘俊重
 * @Description Hbase的基本接口操作
 * @date 17:53
 */
public class HbaseApi {

    /**
     * 配置ss
     */
    static Configuration config = null;
    private Connection connection = null;
    private Admin admin = null;
    private Table table = null;

    @Before
    public void init() throws Exception {
        config = HBaseConfiguration.create();// 配置
        config.set("hbase.zookeeper.quorum", "mini2,mini3");// zookeeper地址
        config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        table = connection.getTable(TableName.valueOf("user"));
    }

    /**
     * 创建一个表
     *
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        // 创建表管理类
        //HBaseAdmin admin = new HBaseAdmin(config); // hbase表管理
        // 创建表描述类
        TableName tableName = TableName.valueOf("user"); // 表名称
        HTableDescriptor desc = new HTableDescriptor(tableName);
        // 创建列族的描述类
        HColumnDescriptor family = new HColumnDescriptor("baseinfo"); // 列族
        // 将列族添加到表中
        desc.addFamily(family);
        HColumnDescriptor family2 = new HColumnDescriptor("contract"); // 列族
        // 将列族添加到表中
        desc.addFamily(family2);
        // 创建表
        admin.createTable(desc); // 创建表
    }

    /**
     * 删除表
     */
    @Test
    public void removeTable() throws IOException {
        TableName table = TableName.valueOf("user2");
        if(admin.tableExists(table)){
            admin.disableTable(table);
            admin.deleteTable(table);
        }else{
            System.out.println("表不存在");
        }
    }

    /**
     * 添加单条数据
     * @throws IOException
     */
    @Test
    public void addOne() throws IOException {
        Put put = new Put(Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"),Bytes.toBytes("lucy"));
        table.put(put);
    }
    /**
     * 向表中插入多条数据
     */
    @Test
    public void addDatas() throws Exception {
        ArrayList<Put> arrayList = new ArrayList<>();
        for(int i=1;i<10;i++){
            Put put = new Put(Bytes.toBytes("3_"+i));
            put.addColumn(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"),Bytes.toBytes("lisi_"+i));
            put.addColumn(Bytes.toBytes("contract"),Bytes.toBytes("phone"),Bytes.toBytes("1830070518"+i));
            arrayList.add(put);
        }
        //插入数据
        table.put(arrayList);
    }

    /**
     * 更新数据，插入一条rowkey相同的数据就是更新
     */
    @Test
    public void updateData() throws IOException {
        Put put = new Put(Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"),Bytes.toBytes("hanmeimei"));
        put.addColumn(Bytes.toBytes("contract"),Bytes.toBytes("phone"),Bytes.toBytes("763837271"));
        table.put(put);
    }

    /**
     * 根据主键删除数据
     * @throws IOException
     */
    @Test
    public void deleteData() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("2"));
        table.delete(delete);

    }

    /**
     * 查询单条数据
     * @throws IOException
     */
    @Test
    public void getOne() throws IOException {
        Get getOne = new Get(Bytes.toBytes("2"));
        Result result = table.get(getOne);
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("contract"),Bytes.toBytes("phone"))));
    }


    /**
     * 全表扫描数据
     */
    @Test
    public void scanData() throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("3_0"));
        scan.setStopRow(Bytes.toBytes("3_9"));
        ResultScanner results = table.getScanner(scan);
        for(Result result : results){
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("contract"),Bytes.toBytes("phone"))));
        }
    }

    //-----------------------------过滤器

    /**
     * 全表扫描的列值过滤器
     */
    @Test
    public void scanDataFilter1() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter scf = new SingleColumnValueFilter(Bytes.toBytes("baseinfo"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("lisi_1"));
        scan.setFilter(scf);
        ResultScanner results = table.getScanner(scan);
        for(Result result : results){
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("contract"),Bytes.toBytes("phone"))));
        }
    }

    /**
     * 全表扫描的rowkey过滤器
     */
    @Test
    public void scanDataFilter2() throws IOException {
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^3"));
        scan.setFilter(rowFilter);
        ResultScanner results = table.getScanner(scan);
        for(Result result : results){
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("contract"),Bytes.toBytes("phone"))));
        }
    }

    /**
     * 全表扫描的列名前缀过滤器
     */
    @Test
    public void scanDataFilter3() throws IOException {
        Scan scan = new Scan();
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("na"));
        scan.setFilter(columnPrefixFilter);
        ResultScanner results = table.getScanner(scan);
        for(Result result : results){
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("contract"),Bytes.toBytes("phone"))));
        }
    }

    /**
     * 过滤器集合
     */
    @Test
    public void scanDataFilter4() throws IOException {
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //前缀过滤器
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("na_"));
        filterList.addFilter(columnPrefixFilter);
        //列值过滤器
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("contract"),
                Bytes.toBytes("phone"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("18300705185"));
        filterList.addFilter(singleColumnValueFilter);
        scan.setFilter(filterList);
        ResultScanner results = table.getScanner(scan);
        for(Result result : results){
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("contract"),Bytes.toBytes("phone"))));
        }
    }

    @After
    public void close() throws Exception {
        table.close();
        admin.close();
        connection.close();
    }
}
