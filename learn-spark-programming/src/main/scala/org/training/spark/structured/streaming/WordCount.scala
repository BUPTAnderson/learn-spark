package org.training.spark.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.training.Contants

/**
  * Created by anderson on 17-10-20.
  * I. 本地运行
  * 1. 本地直接运行当前类
  * 2. 当开终端执行: nc -lp 9999发送数据
  * 3. 查看输出
  *
  * II 集群模式
  * 1. 启动dfs服务, spark服务, metastore服务
  * 2. cp /home/anderson/GitHub/learn-spark/learn-spark-programming/target/learn-spark-programming-1.0-SNAPSHOT.jar  structedstreamingwordcount.jar
  * 3. 编写执行脚本:
cat structedstreamingwordcount.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.structured.streaming.WordCount \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/structedstreamingwordcount.jar

  * 4. 打开终端执行: nc -lp 9999
  * 5. ./structedstreamingwordcount.sh
  * 6. 执行nc命令的窗口发送数据
  * 7. 执行./structedstreamingwordcount.sh的窗口查看输出
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    // lines是dataframe
    val lines = spark.readStream
      .format("socket")
      .option("host", Contants.HOSTNAME)
      .option("port", 9999)
      .load()

    // Split the lines into words
    // 通过as将dataframe转换为dataset
    val words = lines.as[String].flatMap(_.split("\\s+"))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()
//    val wordCounts = words.withWatermark("value", "20 seconds").groupBy("", window($"timestamp", "10 minutes", "5 minutes")).count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
//      .trigger(Trigger.ProcessingTime("20 seconds"))
//      .outputMode("append")
//      .format("console")
//      .option("truncate", false)
//      .start()

    query.awaitTermination()
  }
}
