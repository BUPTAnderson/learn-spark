package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.State
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.StreamingContext
import org.training.Contants

/**
  * Created by anderson on 17-10-17.
  * 1.需要启动dfs服务, 由于是local模式, 不需要启动spark 服务(不需要启动 start-all.sh)
  * 2. cp learn-spark-programming-1.0-SNAPSHOT.jar mapwith.jar
  * 3. 编写启动脚本:
cat mapwith.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.streaming.MapWithStateDemo \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/mapwith.jar

  * 4. 在一个终端使用: nc -lp 9999, 来发送命令, 如果使用(nc -lk 9999, 会报Error connecting to localhost:9999 - java.net.ConnectException: Connection refused)
  * 5. 启动: ./mapwith.sh
  * 6. 在nc窗口发送数据即可, 比如: hadoop  hadoop(注意\t分割, 因为程序中是使用的\t来处理的)
  */
object MapWithStateDemo {
  def main(args: Array[String]): Unit = {
    /**
      * local[1]  中括号里面的数字都代表的是启动几个工作线程
      * 默认情况下是一个工作线程。那么做为sparkstreaming 我们至少要开启
      * 两个线程，因为其中一个线程用来接收数据，这样另外一个线程用来处理数据。
      */
    val conf=new SparkConf().setMaster("local[2]").setAppName("MapWithStateDemo")
    /**
      * Seconds  指的是每次数据数据的时间范围 （bacth interval）
      */
    val  ssc=new StreamingContext(conf,Seconds(2));
    ssc.checkpoint(".")

    val fileDS=ssc.socketTextStream(Contants.HOSTNAME, 9999)
    val wordDstream =fileDS.flatMap { line => line.split("\t") }
      .map { word => (word, 1) }

    /**
      * word: String, one: Option[Int], state: State[Int]
      * 这个函数里面有三个参数
      * 第一个参数：word: String  代表的就是key
      * 第二个参数：one: Option[Int] 代表的就是value
      * 第三个参数：state: State[Int] 代表的就是状态（历史状态，也就是上次的结果）
      *
      * hello,4
      *
      * hello,1
      *
      * hello,5
      */
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    /**
      * hello,1
      * hello,2
      * world,2
      */
    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))

    /**
      * 打印RDD里面前十个元素
      */
    //  wordcount.print()
    stateDstream.print()
    //启动应用
    ssc.start()
    //等待任务结束
    ssc.awaitTermination()
  }
}
