package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

/**
  * Created by anderson on 17-10-17.
  * 1.需要启动dfs服务, 由于是local模式, 不需要启动spark 服务(不需要启动 start-all.sh)
  * 2. cp learn-spark-programming-1.0-SNAPSHOT.jar networdcountupdate.jar
  * 3. 编写启动脚本:
cat update.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.streaming.NetWordCountUpdateStateByKey \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/update.jar

  * 4. 在一个终端使用: nc -lp 9999, 来发送命令, 如果使用(nc -lk 9999, 会报Error connecting to localhost:9999 - java.net.ConnectException: Connection refused)
  * 5. 启动: ./update.sh
  * 6. 在nc窗口发送数据即可, 比如: hadoop  hadoop(注意\t分割, 因为程序中是使用的\t来处理的)
  */
object NetWordCountUpdateStateByKey {
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

    /**
      * !!! 需要设置一个checkpoint的目录
      * 因为我们的计算结果有中间状态，这些中间状态需要存储
      */
    ssc.checkpoint(".")
    val wordDS = fileDS.flatMap(line => line.split("\t"))
      .map(word => (word, 1))

    /**
      * updateFunc: (Seq[Int], Option[S]) => Option[S]
      * updateFunc 这是一个匿名函数
      *  (Seq[Int], Option[S]) 两个参数
      *  Option[S] 返回值
      *  首先我们考虑一个问题
      *  wordDS  做的bykey的计算，说明里面的内容是tuple类型，是键值对的形式，说白了是不是
      *  就是【K V】
      *  wordDS[K,V]
      *  (Seq[Int], Option[S])
      *  参数一：Seq[Int] Seq代表的是一个集合，int代表的是V的数据类型
      *  			---分组的操作，key相同的为一组 (hadoop,{1,1,1,1})
      *  参数二：Option[S] S代表的是中间状态State的数据类型，S对于我们的这个wordcount例子来讲，应该是
      *  int类型。中间状态存储的是单词出现的次数。 hadoop -> 4
      *
      *  Option[S] 返回值  应该跟中间状态一样吧。
      *  Option Some/None
      *
      */
    val wordcountDS = wordDS.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum; // 获取此次本单词出现的次数
      val count = state.getOrElse(0)
      Some(currentCount + count)
    })
    wordcountDS.print()
    /**
      * 打印RDD里面前十个元素
      */
    // 启动应用
    ssc.start()
    // 等待任务结束
    ssc.awaitTermination()
  }
}
