package com.catchu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 直接从mysql数据库中读dataframe数据
 */
public class DataFrameWithJdbc {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("DataFrameWithJdbc");
        conf.setMaster("local");
        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String,String> options = new HashMap<String,String>();
        options.put("url","jdbc:mysql://192.168.8.101:3306/spark");
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("user","root");
        options.put("password","123456");
        options.put("dbtable","person");
        Dataset<Row> personDf = sqlContext.read().format("jdbc").options(options).load();
        personDf.show();
        personDf.registerTempTable("person");

        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url","jdbc:mysql://192.168.8.101:3306/spark");
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","123456");
        reader.option("dbtable","score");
        Dataset<Row> scoreDf = reader.load();
        scoreDf.show();
        scoreDf.registerTempTable("score");

        Dataset<Row> result = sqlContext.sql("select person.id,person.name,person.age,score.score from person,score where person.name = score.name");
        result.show();

        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123456");
        //将计算结果写回到数据库
        result.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.8.101:3306/spark","result",properties);

        sc.stop();
    }
}
