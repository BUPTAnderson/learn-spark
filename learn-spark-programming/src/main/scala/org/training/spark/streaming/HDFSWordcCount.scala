package org.training.spark.streaming

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.training.Contants

/**
  * Created by anderson on 17-10-17.
  * 需要启动dfs服务, 由于是local模式, 不需要启动spark 服务(不需要启动 start-all.sh), 不需要启动yarn服务
  * 1. 创建目录, hadoop fs -mkdir /hdfs
  * 2. cp learn-spark-programming-1.0-SNAPSHOT.jar hdfs.jar
  * 3. 编写启动脚本:
cat hdfs.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.streaming.HDFSWordcCount \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/hdfs.jar

  * 4. 启动脚本: ./hdfs.sh
  * 5. 打开另一个终端上传文件: hadoop fs -put test.txt /hdfs/, hadoop fs -put test2.txt /hdfs/
  * 这是可以看到启动脚本的终端有结果输出
  */
object HDFSWordcCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordcCount")
    val ssc = new StreamingContext(conf, Seconds(2))
    // 指定目录
    val fileDS = ssc.textFileStream(Contants.DEFAULTFS + File.separator + "hdfs")
    val wordcountds = fileDS.flatMap(line => line.split("\t"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordcountds.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
