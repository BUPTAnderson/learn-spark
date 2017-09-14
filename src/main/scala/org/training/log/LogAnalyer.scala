package org.training.log

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by anderson on 17-9-12.
  */
object LogAnalyer {
  def main(args: Array[String]): Unit = {
      // 本地运行
//    val conf = new SparkConf().setMaster("local").setAppName("LogAnalyer")
      // spark submit
    val conf = new SparkConf().setAppName("LogAnalyer")
    val sc = new SparkContext(conf)
      // 读取本地文件
//    val logsRDD = sc.textFile("/home/anderson/log.txt")
    // 读取hdfs
    val logsRDD = sc.textFile("hdfs://anderson-JD:9000/logs/")
      .map(line => ApacheAccessLog.parseLog(line))
      .cache()

    /**
      * 需求一
      * The average, min, and max content size of responses returned from the server.
      */
    val contextSize = logsRDD.map(log => log.contentSize)
    // get max
    val maxSize = contextSize.max()
    // get min
    val minSize = contextSize.min()
    // total/count
    // get average
    val averageSize = contextSize.reduce(_ + _)/contextSize.count()
    println("======================需求一===============================")
    println("最大值: " + maxSize + ", 最小值: " + minSize + ", 平均值: " + averageSize)

    /**
      * 需求二
      * A count of response code's returned.
      */
    println("======================需求二===============================")
    logsRDD.map(log => (log.responseCode, 1))
      .reduceByKey(_ + _)
      .foreach(result => println("状态码:" + result._1 + ", 出现次数:" + result._2))

    /**
      * 需求三
      * All IPAddresses that have accessed this server more than N times.
      */
    println("======================需求三===============================")
    val result = logsRDD.map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(result => result._2 > 2) // 大于某个值
      .take(2) // 取前几个值
    for(tupe <- result) {
      println("ip: " + tupe._1 + ", 出现的次数:" + tupe._2)
    }
    Thread.sleep(200000)
    /**
      * 需求四
      * The top endpoints requested by count.  TopN
      */
    println("======================需求四===============================")
    val topN = logsRDD.map(log => (log.endPoint, 1))
      .reduceByKey(_ + _)
      .map(result => (result._2, result._1))
      .sortByKey(false)
      .take(2)
    for(tuple <- topN) {
      println("目标地址: " + tuple._2 + ", 出现的次数:" + tuple._1)
    }
    logsRDD.unpersist(true)
  }
}
