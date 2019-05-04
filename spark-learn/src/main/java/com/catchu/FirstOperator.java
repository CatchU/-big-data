package com.catchu;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 第一个算子，包含transformations和Action算子
 */
public class FirstOperator {

    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
        sc.setAppName("FirstOperator");
        sc.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sc);
        JavaRDD<String> textFileRDD = context.textFile("./words");

        //TransFormation算子
        JavaRDD<String> flatMapRDD = textFileRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                //System.out.println("+++++++++++++++++++++++");
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //Transformation算子
        JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                //System.out.println("======================");
                return new Tuple2<>(s, 1);
            }
        });

//        //随机抽样算子
//        JavaPairRDD<String, Integer> sample = pairRDD.sample(false, 0.1,100);

        JavaPairRDD<String, Integer> reduceByKeyRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

//        JavaPairRDD<String, Integer> filter = reduceByKeyRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
//                return "redis".equals(tuple2._1());
//            }
//        });

        JavaPairRDD<String, Integer> sortByKeyRDD = reduceByKeyRDD.sortByKey(true);
//        long count = sample.count();
//        System.out.println(count);


        sortByKeyRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });

    }
}
