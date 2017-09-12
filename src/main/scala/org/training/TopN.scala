package org.training

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by anderson on 17-9-12.
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TopN")
    val sc = new SparkContext(conf)
    val file = sc.textFile("/home/anderson/hello.txt")
    val topN = file.flatMap(line => line.split("\t"))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(false)
      .take(3)
    for (i <- topN) {
      println(i._2 + " 出现次数:" + i._1)
    }
  }
}
