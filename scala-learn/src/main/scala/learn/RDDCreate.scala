package learn


import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * rdd的创建
  */
object RDDCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDCreate").setMaster("local");
    val context = new SparkContext(conf)

    //使用textFile读取文件创建
    val lines: RDD[String] = context.textFile("./words")

    //使用parallelize创建rdd
    val rdd: RDD[Int] = context.parallelize(Array(1,2,3,4,5))
    rdd.foreach(println)

    //使用makeRDD创建rdd
    val pairRdd: RDD[(String, Int)] = context.makeRDD(Array(("zhangsan",1),("lisi",2),("wangwu",3)),2)
    pairRdd.foreach(println)
  }
}
