package com.catchu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

public class PvStatistic {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("PvStatistic");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile("./pvuvdata");
        JavaPairRDD<String, Integer> domains = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<>(line.split("\t")[5], 1);
            }
        });

        //统计每个域名出现的个数
        JavaPairRDD<String, Integer> domainReduce = domains.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        //调转顺序准备排序
        JavaPairRDD<Integer, String> domainSwap = domainReduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        //根据key进行排序
        JavaPairRDD<Integer, String> sortedDomain = domainSwap.sortByKey(false);

        //排序好的结果交换顺序
        JavaPairRDD<String, Integer> resultDomain = sortedDomain.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        List<Tuple2<String, Integer>> take = resultDomain.take(5);
        for(Tuple2<String, Integer> tuple2 : take){
            System.out.println("key:"+tuple2._1+" - value:"+tuple2._2);
        }
    }
}
