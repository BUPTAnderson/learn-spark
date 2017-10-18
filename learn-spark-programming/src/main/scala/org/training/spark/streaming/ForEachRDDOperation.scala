package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.training.Contants

/**
  * Created by anderson on 17-10-17.
  *
  * 1. 打开终端, 执行: nc -lp 9999
  * 2. 直接运行当前类,
  * 3. 在终端中输入数据进行测试
  * 4. 查看mysql中是否有数据插入
  */
object ForEachRDDOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ForEachRDDOperation")

    val ssc = new StreamingContext(conf,Seconds(2));

    val fileDS = ssc.socketTextStream(Contants.HOSTNAME, 9999);
    val wordcountDS = fileDS.flatMap(line => line.split("\t"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordcountDS.foreachRDD(partitionOfRecord => {
      partitionOfRecord.foreachPartition(records => {
        val connect = MysqlPool.getJdbcConnect()
        while (records.hasNext) {
          val tuple = records.next()
          val sql = "insert into wordcount values(now(), '" + tuple._1 + "', " + tuple._2.toInt + ")";
          val statement = connect.createStatement();
          statement.executeUpdate(sql)
          print(sql)
        }
        MysqlPool.releaseConn(connect)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
