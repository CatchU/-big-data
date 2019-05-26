package com.catchu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * 读或者写parquet数据
 */
public class DataFrameWithParquet {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("DataFrameWithParquet");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> jsonRdd = sc.textFile("./sparksql/json");
        Dataset<Row> jsonRow = sqlContext.read().json(jsonRdd);

        //写成parquet格式的数据
        jsonRow.write().mode(SaveMode.Overwrite).parquet("./sparksql/parquet");
        jsonRow.show();

        System.out.println("============读取parquet格式数据");
        sqlContext.read().parquet("./sparksql/parquet").show();
        sc.close();
    }
}
