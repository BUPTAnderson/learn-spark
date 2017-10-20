package org.training.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by anderson on 17-10-18.
  * I. 本地启动
  * 1. 直接启动当前类
  * 2. 打开终端, 执行: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
  * 向topic中发送消息
  * 3. 终端输出结果
  *
  * II.
  * 1. 启动hdfs, start-dfs.sh ; 启动spark集群: start-all.sh
  * 2. (由于依赖其它jar, 打依赖包)cp /home/anderson/GitHub/learn-spark/learn-spark-programming/target/learn-spark-programming-1.0-SNAPSHOT-jar-with-dependencies.jar kafka.jar
  * 3. 编写启动脚本
cat kafka.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.streaming.KafkaOperation \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/kafka.jar

  * 4. 启动脚本
  * 5. 打开终端, 执行: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
  * 向topic中发送消息
  * 6. 查看输出
  */
object KafkaOperation {
  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaOperation")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(".")

    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

//    val topics = Array("topicA", "topicB")
    val topics = Array("test")

    /**
      * k:其实就是偏移量 offset
      * V:就是我们消费的数据
      * InputDStream[(K, V)]
      *
      * k:数据类型
      * v:数据类型
      *
      * k的解码器
      * v的解码器
      * [K, V, KD <: Decoder[K], VD <: Decoder[V]]
      */
    val kafkDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(record => record.value())

    val wordcountDS = kafkDS.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordcountDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
