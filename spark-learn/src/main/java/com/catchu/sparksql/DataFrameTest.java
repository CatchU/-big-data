package com.catchu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("DataFrameTest");
        SparkContext context = new SparkContext(conf);

        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> json = sqlContext.read().format("json").load("./sparksql/json");
        json.printSchema();
        json.show();

//        JavaRDD<Row> javaRDD = json.javaRDD();
//        javaRDD.foreach(new VoidFunction<Row>() {
//            @Override
//            public void call(Row row) throws Exception {
//                System.out.println(row);
//            }
//        });

        //select name from table
        json.select(json.col("name")).show();
        //select nama,age+10 as ageTen from table
        json.select(json.col("name"),json.col("age").plus(10).alias("ageTen")).show();
        //select name,age from table where age>19
        json.select(json.col("name"),json.col("age")).where(json.col("age").gt(19)).show();
        //select name,count(*) from table group by name
        json.groupBy(json.col("age")).count().show();

        //将table注册成一个临时表，这张表并不会雾化到磁盘，只是指针指向原表
        json.registerTempTable("temp_table");
        sqlContext.sql("select name,count(*) from temp_table group by name").show();

        context.stop();

    }
}
