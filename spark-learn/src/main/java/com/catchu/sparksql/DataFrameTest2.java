package com.catchu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

public class DataFrameTest2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("DataFrameTest2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> nameRdd = sc.parallelize(Arrays.asList("{\"name\":\"zhangsan\",\"age\":15}",
                "{\"name\":\"lisi\",\"age\":14}",
                "{\"name\":\"wangwu\",\"age\":18}"));

        JavaRDD<String> scoreRdd = sc.parallelize(Arrays.asList("{\"name\":\"zhangsan\",\"score\":50}",
                "{\"name\":\"lisi\",\"score\":69}",
                "{\"name\":\"wangwu\",\"score\":99}"));

        Dataset<Row> nameDf = sqlContext.read().json(nameRdd);
        Dataset<Row> scoreDf = sqlContext.read().json(scoreRdd);

        nameDf.registerTempTable("tb_name");
        scoreDf.registerTempTable("tb_score");
        sqlContext.sql("select tb_name.name,tb_name.age,tb_score.score from tb_name left join tb_score on tb_name.name = tb_score.name order by tb_score.score desc").show();
    }
}
