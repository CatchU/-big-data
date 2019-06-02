package com.catchu.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SparkStreamingTest {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaReceiverInputDStream<String> inputDStream = jsc.socketTextStream("mini1", 9999);
        JavaDStream<String> words = inputDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> wordMapToPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> dStreamByKey = wordMapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //dStreamByKey.print(1000);
        dStreamByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                //这段代码是在driver端执行
                JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<>(tuple2._1 + "~", tuple2._2);
                    }
                });

                //必须得有action算子触发上面的代码才能执行
                pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println(tuple2);
                    }
                });
            }
        });
        //开始监听，
        jsc.start();
        jsc.awaitTermination();

        //在这里这段代码不会停止，即程序会一直执行
        jsc.stop();
    }
}
