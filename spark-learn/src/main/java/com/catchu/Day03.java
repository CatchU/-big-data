package com.catchu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Day03 {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
        sc.setMaster("local").setAppName("Day03");
        JavaSparkContext context = new JavaSparkContext(sc);

        JavaRDD<String> rdd = context.parallelize(Arrays.asList(
                "love1", "love2", "love3", "love4",
                "love5", "love6", "love7", "love8",
                "love9", "love10", "love11", "love12"
        ), 4);

        JavaRDD<String> partitionsWithIndex = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> ss) throws Exception {
                List<String> list = new ArrayList<>();
                while (ss.hasNext()) {
                    String next = ss.next();
                    list.add("partition:【" + index + "】 - value:【" + next + "】");
                }
                return list.iterator();
            }
        }, true);

        //JavaRDD<String> repartition = partitionsWithIndex.repartition(2);

        JavaRDD<String> repartition = partitionsWithIndex.coalesce(3,true);

        JavaRDD<String> rdd1 = repartition.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> ss) throws Exception {
                List<String> list = new ArrayList<>();
                while (ss.hasNext()) {
                    String next = ss.next();
                    list.add("new Partition :" + index + "-value:" + next);
                }
                return list.iterator();
            }
        }, true);
        List<String> collect = rdd1.collect();
        for(String s : collect){
            System.out.println(s);
        }


        context.close();
    }
}
