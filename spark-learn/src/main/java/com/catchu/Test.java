package com.catchu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a","a","b","b","b","c","d"));


        /**
         * distinct
         */
        JavaRDD<String> distinct = rdd.distinct();
        distinct.foreach(new VoidFunction<String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String arg0) throws Exception {
                System.out.println(arg0);
            }
        });
    }
}
