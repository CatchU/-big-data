package learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FirstOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("ScalaWordCount")

    val context = new SparkContext(conf);
    val lines: RDD[String] = context.textFile("./words")

//    val words: RDD[String] = lines.flatMap(line => {
//      line.split(" ");
//    })
//    words.foreach(println);

//    val sample: RDD[String] = lines.sample(true,0.1)
//    sample.foreach(println);

//    val filter: RDD[String] = lines.filter(s => {
//      s.equals("hello mysql");
//    })
//    filter.foreach(println);

//    val strings: Array[String] = lines.collect()
//    strings.foreach(println)

    val ss: Array[String] = lines.take(5)
    ss.foreach(println)
  }
}
