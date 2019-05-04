package learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第二次学习算子
  */
object SecondOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SecondOperator")
    val context = new SparkContext(conf)

    val rdd1 = context.makeRDD(Array(("zhangsan",18),("lisi",19),("wangwu",20),("zhangsan",18),("lisi",19)),2)
    val rdd2 = context.makeRDD(Array(("zhangsan",18),("lisi",101),("zhaoliu",102)),2)
    //rdd1.join(rdd2).foreach(println)

    //rdd1.leftOuterJoin(rdd2).foreach(println)

    //rdd1.union(rdd2).foreach(println)

    //rdd1.intersection(rdd2).foreach(println)

    //rdd2.subtract(rdd1).foreach(println)

    //rdd1.distinct().foreach(println)

    rdd2.mapPartitions(iter => {
      print("入库"+iter)
      iter
    },true).collect()
  }
}
