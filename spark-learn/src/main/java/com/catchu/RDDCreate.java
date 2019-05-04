package com.catchu;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * rdd的创建方式
 */
public class RDDCreate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("RDDCreate");
        JavaSparkContext context = new JavaSparkContext(conf);

        //读取文件创建
        JavaRDD<String> lines = context.textFile("./words");

        //parallelize创建rdd
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> javaRDD = context.parallelize(list, 2);
        System.out.println(javaRDD.count());

        //parallelizePairs 创建k，v格式的rdd
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 1),
                new Tuple2<String, Integer>("lisi", 2),
                new Tuple2<String, Integer>("wangwu", 3)
        );
        JavaPairRDD<String, Integer> pairRDD = context.parallelizePairs(tuple2s);
        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
    }
}
