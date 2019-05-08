package com.catchu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class Day03Transformation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Day03Transformation").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> rdd1 = context.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("zhangsan", "18"),
                new Tuple2<String, String>("zhangsan", "18"),
                new Tuple2<String, String>("lisi", "20"),
                new Tuple2<String, String>("lisi", "30"),
                new Tuple2<String, String>("lisi", "40"),
                new Tuple2<String, String>("wangwu", "55")
        ));

        JavaPairRDD<String, String> rdd2 = context.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("zhangsan1", "18"),
                new Tuple2<String, String>("zhangsan2", "28"),
                new Tuple2<String, String>("lisi1", "20"),
                new Tuple2<String, String>("lisi2", "30"),
                new Tuple2<String, String>("lisi3", "40"),
                new Tuple2<String, String>("wangwu1", "55")
        ));

        //groupByKey 根据key进行分组
//        JavaPairRDD<String, Iterable<String>> groupByKey = rdd1.groupByKey();
//        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
//            @Override
//            public void call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        //zip将两个rdd进行压缩，个数必须相等
//        JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> zip = rdd1.zip(rdd2);
//        zip.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>() {
//            @Override
//            public void call(Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

//        JavaPairRDD<Tuple2<String, String>, Long> zipWithIndex = rdd1.zipWithIndex();
//        zipWithIndex.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Long>>() {
//            @Override
//            public void call(Tuple2<Tuple2<String, String>, Long> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        //countByKey 根据key进行分组计数
//        Map<String, Long> countByKey = rdd1.countByKey();
//        Set<Map.Entry<String, Long>> entries = countByKey.entrySet();
//        for(Map.Entry<String, Long> entry : entries){
//            System.out.println("key:"+entry.getKey()+" - value:"+entry.getValue());
//        }

        //countByValue 根据key和value组成的pair组进行分组并计数
//        Map<Tuple2<String, String>, Long> map = rdd1.countByValue();
//        Set<Map.Entry<Tuple2<String, String>, Long>> entries = map.entrySet();
//        for(Map.Entry<Tuple2<String, String>, Long> entry : entries){
//            System.out.println("key:"+entry.getKey()+" - value:"+entry.getValue());
//        }

        JavaRDD<Integer> parallelize = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        Integer reduce = parallelize.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        System.out.println(reduce);

        context.stop();
    }
}
