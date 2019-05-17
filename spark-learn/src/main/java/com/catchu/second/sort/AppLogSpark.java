package com.catchu.second.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class AppLogSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("AppLogSpark").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile("./app-log.txt");
        JavaPairRDD<String, AccessLogInfo> logInfoJavaPairRDD = mapToPair(lines);
        JavaPairRDD<String, AccessLogInfo> aggregateData = aggregateByDeviceId(logInfoJavaPairRDD);
        JavaPairRDD<AccessLogInfo, String> swapData = swapData(aggregateData);

        //排序
        JavaPairRDD<AccessLogInfo, String> sortData = swapData.sortByKey(false);
        List<Tuple2<AccessLogInfo, String>> result = sortData.take(10);
        for(Tuple2<AccessLogInfo, String> t : result){
            System.out.println(t._2 + "\t" + t._1.getTimestamp()+"\t"+t._1.getUpTraffic()+"\t"+t._1.getDownTraffic());
        }

        context.close();
    }

    //将读取的每一行数据处理成 deviceId - AccessLogInfo的格式
    private static JavaPairRDD<String, AccessLogInfo> mapToPair(JavaRDD<String> lines){
        return lines.mapToPair(new PairFunction<String, String, AccessLogInfo>() {
            @Override
            public Tuple2<String, AccessLogInfo> call(String accessLog) throws Exception {
                String[] split = accessLog.split("\t");
                Long timestamp = Long.parseLong(split[0]);
                String deviceId = split[1];
                Integer upTraffic = Integer.parseInt(split[2]);
                Integer downTraffic = Integer.parseInt(split[3]);
                AccessLogInfo logInfo = new AccessLogInfo().setTimestamp(timestamp).setUpTraffic(upTraffic).setDownTraffic(downTraffic);
                return new Tuple2<>(deviceId,logInfo);
            }
        });
    }

    //根据key处理聚合数据
    private static JavaPairRDD<String, AccessLogInfo> aggregateByDeviceId(JavaPairRDD<String, AccessLogInfo> logInfoJavaPairRDD){
        return logInfoJavaPairRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
            @Override
            public AccessLogInfo call(AccessLogInfo v1, AccessLogInfo v2) throws Exception {
                Long timestamp = v1.getTimestamp() <= v2.getTimestamp() ? v1.getTimestamp() : v2.getTimestamp();
                int upTraffic = v1.getUpTraffic() + v2.getUpTraffic();
                int downTraffic = v1.getDownTraffic() + v2.getDownTraffic();
                return new AccessLogInfo(timestamp, upTraffic, downTraffic);
            }
        });
    }

    // 将数据的key value进行转换
    private static JavaPairRDD<AccessLogInfo, String> swapData(JavaPairRDD<String, AccessLogInfo> aggregateData){
        return aggregateData.mapToPair(new PairFunction<Tuple2<String, AccessLogInfo>, AccessLogInfo, String>() {
            @Override
            public Tuple2<AccessLogInfo, String> call(Tuple2<String, AccessLogInfo> tuple2) throws Exception {
                return tuple2.swap();
            }
        });
    }

}
