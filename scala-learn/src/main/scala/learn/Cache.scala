package learn

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * cache 懒加载执行，将数据存储在内存中
  *
  * persist 可以指定持久化的位置
  *
  *
  */
object Cache {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("Cache")
    val context = new SparkContext(conf)
    context.setCheckpointDir("./checkoutpoint")

//    val lines: RDD[String] = context.textFile("./NASA_access_log_Aug95")
    val lines: RDD[String] = context.textFile("./words")
    //加入缓存，也是懒执行
    //lines.cache()

    //lines.persist(StorageLevel.MEMORY_ONLY)

    lines.checkpoint()
    lines.collect()

//    val start1: Long = System.currentTimeMillis()
//    val total1: Long = lines.count()
//    val end1: Long = System.currentTimeMillis()
//    println("第一次执行时长:"+(end1-start1)+" 数据量:"+total1)
//
//    val start2: Long = System.currentTimeMillis()
//    val total2: Long = lines.count()
//    val end2: Long = System.currentTimeMillis()
//    println("第二次执行时长:"+(end2-start2)+" 数据量:"+total2)

    //context.stop()

  }

}
