package com.catchu;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * uv统计
 */
public class UvStatistic {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("UvStatistic");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile("./pvuvdata");

        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<>(line.split("\t")[5] + "_" + line.split("\t")[0], 1);
            }
        });

        JavaPairRDD<String, Integer> distinct = pairRDD.distinct();
        JavaPairRDD<String, Integer> pairRDD1 = distinct.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1.split("_")[0], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> pairRDD2 = pairRDD1.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> pairRDD3 = pairRDD2.sortByKey(false);

        JavaPairRDD<String, Integer> pairRDD4 = pairRDD3.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        pairRDD4.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });

        context.close();
    }
}
