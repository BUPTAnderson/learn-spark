package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume.FlumeUtils
import org.training.Contants

/**
  * Created by anderson on 17-10-19.
  * I. 本地执行
  * 1. 编写flume.properties, 见resources中flume.properties
  * 2. 将当前工程的依赖: spark-streaming-flume-sink_2.11-2.2.0.jar, 放到flume的lib下, 将当前工程的
  * scala-library-2.11.8.jar, 替换掉flume lib下的scala-library-2.10.5.jar
  * 3. 启动flume: bin/flume-ng agent -n a1 -f conf/flume.properties
  * 4. 启动当前类
  * 5. 打开终端, 执行命令: echo "hello world hello" >> server.log
  *
  * II. 集群执行
  * 1. 启动hdfs, start-dfs.sh ; 启动spark集群: start-all.sh
  * 2. (由于依赖其它jar, 打依赖包)cp /home/anderson/GitHub/learn-spark/learn-spark-programming/target/learn-spark-programming-1.0-SNAPSHOT-jar-with-dependencies.jar flume.jar
  * 3. 编写启动脚本
cat flume.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.streaming.FlumeWordcount \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/flume.ja

  * 4. 启动flume: bin/flume-ng agent -n a1 -f conf/flume.properties
  * 4. 启动脚本flume.sh
  * 5. 打开终端, 执行命令: echo "hello world hello" >> server.log
  * 6. 查看输出
  */
object FlumeWordcount {
  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaOperation")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")

    val flumeDS = FlumeUtils.createPollingStream(ssc, Contants.HOSTNAME, 9999)
      .map(event => new String(event.event.getBody.array()))

    val wcDS = flumeDS.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wcDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
