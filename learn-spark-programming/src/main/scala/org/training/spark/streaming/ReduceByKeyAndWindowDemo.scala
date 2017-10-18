package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.training.Contants

/**
  * Created by anderson on 17-10-17.
  * 每隔10秒实时统计前20秒的单词计数情况
  * 程序中设置的是每2秒生成一个RDD, 这里的10秒需要是2秒的倍数
  * 假设程序开始时刻是a(程序中打印出了改时间), 则a+10秒后会输出一次数据, a+20秒后输出第二次数据, 依次类推
  * 显然, 第一次a+10秒输出前20秒的数据时, 只能输出到程序开始时刻a到a+10这段时间内获取到的数据
  * 第二次a+20秒输出数据的时候, 输出的是a到a+20这20秒内获取到的数据
  *
  * 1. 打开终端, 执行: nc -lp 9999
  * 2. 直接运行当前类
  */
object ReduceByKeyAndWindowDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("ReduceByKeyAndWindowDemo")
    val  ssc=new StreamingContext(conf,Seconds(2));
    val fileDS=ssc.socketTextStream(Contants.HOSTNAME, 9999);
    println("-------------------------" + System.currentTimeMillis())

    val wordcountDS = fileDS.flatMap(line => line.split("\t"))
      .map(word => (word, 1))
      .reduceByKeyAndWindow((x: Int, y: Int) => {x + y}, Seconds(20), Seconds(10))  //(_+_)

    wordcountDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
