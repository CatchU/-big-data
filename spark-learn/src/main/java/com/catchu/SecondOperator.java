package com.catchu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 第二次学算子
 */
public class SecondOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SecondOperator");
        conf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> rdd1 = context.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", 1),
                new Tuple2<>("lisi", 2),
                new Tuple2<>("wangwu", 3),
                new Tuple2<>("zhaoliu",4)),2);

        JavaPairRDD<String, String> rdd2 = context.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", "a"),
                new Tuple2<>("lisi", "b"),
                new Tuple2<>("wangwu", "c"),
                new Tuple2<>("sunqi","d")),3);

        JavaPairRDD<String, String> rdd3 = context.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", "a"),
                new Tuple2<>("lisi", "200"),
                new Tuple2<>("wangwu", "300"),
                new Tuple2<>("zhaoliu","400")),4);

//        System.out.println(rdd1.getNumPartitions());

//        JavaPairRDD<String, Tuple2<Integer, String>> joinRdd = rdd1.join(rdd2);
//        joinRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, String>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Integer, String>> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

//        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> leftOuterJoin = rdd1.leftOuterJoin(rdd2);
//        leftOuterJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<String>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Integer, Optional<String>>> tuple2) throws Exception {
//                String key = tuple2._1();
//                Tuple2<Integer, Optional<String>> optionalTuple2 = tuple2._2();
//                Integer value1 = optionalTuple2._1();
//                Optional<String> optional = optionalTuple2._2();
//                String s = "";
//                if(optional.isPresent()){
//                    s = optional.get();
//                }
//                System.out.println("key:"+key+" value1:"+value1+" value2:"+s);
//            }
//        });

//        JavaPairRDD<String, Tuple2<Optional<Integer>, String>> rightOuterJoin = rdd1.rightOuterJoin(rdd2);
//        rightOuterJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, String>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Optional<Integer>, String>> tuple2) throws Exception {
//                String key = tuple2._1();
//                Tuple2<Optional<Integer>, String> optionalTuple2 = tuple2._2();
//                Optional<Integer> optional = optionalTuple2._1();
//                Integer i = null;
//                if(optional.isPresent()){
//                    i = optional.get();
//                }
//                String value2 = optionalTuple2._2();
//                System.out.println("key:"+key+" value1:"+i+" value2:"+value2);
//            }
//        });

//        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> outerJoin = rdd1.fullOuterJoin(rdd2);
//        System.out.println("join后的分区数量:"+outerJoin.partitions().size());
//        outerJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Optional<String>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Optional<Integer>, Optional<String>>> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        //union合并rdd，类型一致
//        JavaPairRDD<String, String> union = rdd2.union(rdd3);
//        union.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        //intersection取两个rdd的交集，必须key，value完全一致才算一样
//        JavaPairRDD<String, String> intersection = rdd2.intersection(rdd3);
//        intersection.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        //subtract取两个rdd的差集
//        JavaPairRDD<String, String> subtract = rdd3.subtract(rdd2);
//        subtract.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        //cogroup将连个rdd的key合并，每个key都是对应的avalue集合
//        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroup = rdd2.cogroup(rdd3);
//        cogroup.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> stringTuple2Tuple2) throws Exception {
//                System.out.println(stringTuple2Tuple2);
//            }
//        });

        //map只能一个数据一个数据的操作，比较消耗资源
        JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
//        JavaRDD<String> map = rdd.map(new Function<Integer, String>() {
//            @Override
//            public String call(Integer i) throws Exception {
//                System.out.println("开启数据库连接");
//                System.out.println("模拟入库");
//                System.out.println("关闭连接");
//                System.out.println("=====================");
//                return i + "";
//            }
//        });
//        map.collect();

        //mapPartitions按照分区数量处理数据
//        JavaRDD<String> mapPartitions = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {
//            List<String> list = new ArrayList<>();
//            @Override
//            public Iterator<String> call(Iterator<Integer> iterator) throws Exception {
//                System.out.println("开启连接");
//                while (iterator.hasNext()) {
//                    Integer next = iterator.next();
//                    System.out.println("模拟入库:"+next);
//                    list.add(String.valueOf(next));
//                }
//                System.out.println("关闭连接");
//                System.out.println("==============");
//                return list.iterator();
//            }
//        });
//        mapPartitions.collect();

        //foreachPartition按照一个分区一个分区的来遍历数据
        rdd.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> iterator) throws Exception {
                System.out.println("开启连接");
                while (iterator.hasNext()){
                    Integer next = iterator.next();
                    System.out.println("模拟入库:"+next);
                }
                System.out.println("关闭连接");
                System.out.println("==============");
            }
        });
    }
}
