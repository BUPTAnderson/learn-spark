package org.training.skew

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by anderson on 17-9-14.
  */
object AggWordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("AggWordcount")
    val sc = new SparkContext(conf)
    val list = Array("you,jump", "i,jump", "you,jump", "jump,jump", "jump,jump", "jump,jump")
    val listRDD = sc.parallelize(list, 1) // 1个partition
    listRDD.flatMap(line => line.split(","))
      .map(word => (word, 1))
      .map(word => {
        val prefix = (new util.Random()).nextInt(4)
        (prefix + "_" + word._1, word._2)
      })
      .reduceByKey(_ + _)
      .map(word => {
        val key = word._1.split("_")(1)
        (key, word._2)
      })
      .reduceByKey(_ + _)
      .foreach(result => println(result._1 + " --> " + result._2))
  }
}
