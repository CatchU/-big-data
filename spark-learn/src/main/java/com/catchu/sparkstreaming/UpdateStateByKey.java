package com.catchu.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
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
import java.util.List;

/**
 * 统计sparkstreaming程序自启动到目前为止的数据
 */
public class UpdateStateByKey {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        jsc.checkpoint("checkpoint");
        JavaReceiverInputDStream<String> inputDStream = jsc.socketTextStream("mini1", 9999);
        JavaPairDStream<String, Integer> wordMapToPair = inputDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> stateByKey = wordMapToPair.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> list, Optional<Integer> state) throws Exception {
                Integer result = 0;
                if (state.isPresent()) {
                    result = state.get();
                }
                for (Integer i : list) {
                    result += i;
                }
                return Optional.of(result);
            }
        });

        stateByKey.print(1000);

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
