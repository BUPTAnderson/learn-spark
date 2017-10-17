package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-10-13.
  */
object LogAnalySql {
  case class ApacheAccessLog(
                              ipAddress : String, // ip地址
                              clientIdentity : String, // 标识符
                              userId : String, //用户ID
                              dataTime : String, // 时间
                              method : String, // 请求方式
                              endPoint : String, // 请求网址
                              protocol : String, // 协议
                              responseCode : Int, // 网页请求响应类型
                              contentSize : Long // 返回内容长度
                            )

  object ApacheAccessLog {

    def parseLog(log : String): ApacheAccessLog = {
      val logArray = log.split("#");
      val url = logArray(4).split(" ")
      ApacheAccessLog(logArray(0), logArray(1), logArray(2), logArray(3), url(0), url(1), url(2), logArray(5).toInt, logArray(6).toLong)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogAnalySql")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val logsDF = sc.textFile("hdfs://anderson-JD:9000/logs/")
      .map(log => ApacheAccessLog.parseLog(log))
      .toDF()
    logsDF.createOrReplaceTempView("log")

    /**
      * 需求一
      * The average, min, and max content size of responses returned from the server.
      */
    spark.sql("select avg(contentSize), min(contentSize), max(contentSize) from log").show()

    /**
      * 需求二
      * A count of response code's returned.
      */
    spark.sql("select responseCode, count(*) from log group by responseCode limit 10").show()

    /**
      * 需求三
      * All IPAddresses that have accessed this server more than N times.
      */
    spark.sql("select ipAddress, count(1) as total from log group by ipAddress having total > 10").show()

    /**
      * 需求四
      * The top endpoints requested by count.  TopN
      */
    spark.sql("select endPoint, count(1) as total from log group by endPoint order by total desc limit 3").show()
  }
}
