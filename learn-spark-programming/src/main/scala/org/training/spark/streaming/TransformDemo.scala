package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.StreamingContext
import org.training.Contants

/**
  * Created by anderson on 17-10-17.
  * 1. 打开终端, 执行: nc -lp 9999
  * 2. 直接运行当前类,
  * 3. 在终端中输入数据进行测试, 可以发现, ",", "?", "!", "."被过滤掉了
  */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("TransformDemo")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")
    val fileDS = ssc.socketTextStream(Contants.HOSTNAME, 9999)
    val wordcountDS = fileDS.flatMap { line => line.split("\t") }
      .map { word => (word, 1) }

    val filter = ssc.sparkContext.parallelize(List(",", "?", "!", ".")).map(param => (param, true))

    val needwordDS = wordcountDS.transform(rdd => {
      val leftRDD = rdd.leftOuterJoin(filter)

      //leftRDD String,(int,option[boolean]);
      val needword = leftRDD.filter(tuple => {
        //        val x = tuple._1
        val y = tuple._2
        if (y._2.isEmpty) {
          true
        } else {
          false
        }
      })
      needword.map(tuple => (tuple._1, 1))
    })

    //    val wcDS = needwordDS.reduceByKey(_ + _)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val wcDS = needwordDS.mapWithState(
      StateSpec.function(mappingFunc))

    wcDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
