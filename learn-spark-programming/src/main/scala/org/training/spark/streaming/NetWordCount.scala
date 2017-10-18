package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

/**
  * Created by anderson on 17-10-17.
  * 1.需要启动dfs服务, 由于是local模式, 不需要启动spark 服务(不需要启动 start-all.sh)
  * 2. cp learn-spark-programming-1.0-SNAPSHOT.jar networdcount.jar
  * 3. 编写启动脚本:
cat networdcount.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.streaming.NetWordCount \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/networdcount.jar

  * 4. 在一个终端使用: nc -lp 9999, 来发送命令, 如果使用(nc -lk 9999, 会报Error connecting to localhost:9999 - java.net.ConnectException: Connection refused)
  * 5. 启动: ./networdcount.sh
  * 6. 在nc窗口发送数据即可, 比如: hadoop  hadoop(注意\t分割, 因为程序中是使用的\t来处理的)
  */
object NetWordCount {
  def main(args: Array[String]): Unit = {
    /**
      * local[2] 中括号里面的数字代表的是启动几个工作线程
      * 默认情况下是一个工作线程, 那么作为sparkstreaming 我们至少要开启两个线程,
      * 其中一个线程用来接收数据, 这样另一个线程用来处理数据.
      */
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")

    /**
      * Seconds 指的是每次处理数据的时间范围(batch interval)
      */
    val ssc = new StreamingContext(conf, Seconds(1))

    val fileDS = ssc.socketTextStream("anderson-JD", 9999)
    val wordcount = fileDS.flatMap(line => line.split("\t"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    /**
      * 打印RDD里面前十个元素
      */
    wordcount.print()
    // 启动应用
    ssc.start()
    // 等待任务结束
    ssc.awaitTermination()
  }
}
