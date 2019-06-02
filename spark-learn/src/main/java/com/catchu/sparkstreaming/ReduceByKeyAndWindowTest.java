package com.catchu.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 根据窗口长度和滑动间隔统计数据
 */
public class ReduceByKeyAndWindowTest {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKeyAndWindowTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //每隔5秒批处理数据
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        jsc.checkpoint("checkpoint");
        JavaReceiverInputDStream<String> inputDStream = jsc.socketTextStream("mini1", 9999);

        JavaPairDStream<String, Integer> pairs = inputDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

//        JavaPairDStream<String, Integer> pairDStream = pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        }, Durations.seconds(15), Durations.seconds(5));
//
//        pairDStream.print();

        //对滑动窗口进行优化
        JavaPairDStream<String, Integer> reduceByKeyAndWindow = pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            /**
             * 这里的v1是指上一个所有的状态的key的value值（如果有出去的某一批次值，v1就是下面第二个函数返回的值），v2为本次的读取进来的值
             */
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("***********v1*************"+v1);
                System.out.println("***********v2*************"+v2);
                return v1+v2;
            }
        }, new Function2<Integer,Integer,Integer>(){
            /**
             * 这里的这个第二个参数的Function2是在windowLength时间后才开始执行，v1是上面一个函数刚刚加上最近读取过来的key的value值的最新值,
             * v2是窗口滑动后，滑动间隔中出去的那一批值
             * 返回的值又是上面函数的v1 的输入值
             */
            public Integer call(Integer v1, Integer v2) throws Exception {

                System.out.println("^^^^^^^^^^^v1^^^^^^^^^^^^^"+v1);
                System.out.println("^^^^^^^^^^^v2^^^^^^^^^^^^^"+v2);

//				return v1-v2-1;//每次输出结果递减1
                return v1-v2;
            }

        }, Durations.seconds(15), Durations.seconds(5));
        reduceByKeyAndWindow.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
