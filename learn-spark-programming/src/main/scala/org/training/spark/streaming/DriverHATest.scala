package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._

/**
  * Created by anderson on 17-10-19.
  */
object DriverHATest {
  def main(args: Array[String]): Unit = {
    val checkpointDirectory="kafka/"

    def functionToCreateContext(): StreamingContext = {

      val conf=new SparkConf().setMaster("local[2]").setAppName("KafkaOperation")
      val sc=new SparkContext(conf)
      val  ssc=new StreamingContext(sc,Seconds(2))
      ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    ssc.checkpoint(checkpointDirectory)
    val kafkaParams=Map("metadata.broker.list" -> "hadoop1:9092");
    val topics=Set("xtwy");

    val kafkaDS= KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(record => record.value())

    val wordcountDS= kafkaDS.flatMap { line => line.split("\t") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)//window  mapwithstate updatewithstateByKey topK
    wordcountDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
