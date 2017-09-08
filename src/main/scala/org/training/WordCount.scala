package org.training

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by anderson on 17-9-5.
standalone cluster模式
$ cat wordcount.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.WordCount \
  --master spark://anderson-JD:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkcore/wordcount.jar \

standalone  client模式(打印信息比较丰富, 适合测试, 成功后可以直接通过cluster模式运行)
  $ cat wordcount-client.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.WordCount \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 1 \
  /home/anderson/gitJD/sparkcore/wordcount.jar \
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 操作本地文件, 直接运行即可
    // 不设置为local会运行在集群上面, app name(任务名称)也是必须要设置的
    // local可以改为 local[8] 数字是指启动几个本地线程, 不填的话默认是1
//    val conf = new SparkConf().setMaster("local").setAppName("WordCont")
//    val sc = new SparkContext(conf)
//    sc.textFile("/home/anderson/test.txt")
//      .flatMap(line => line.split("\t"))
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//      .foreach(result => println(result._1 + ": " + result._2))

    // 打成jar包运行到spark集群上
    // 统计hdfs上的文件
    val conf = new SparkConf().setAppName("WordCont")
    val sc = new SparkContext(conf)
//    sc.textFile("hdfs://ns1/spark/abc.txt")
    val file = sc.textFile("hdfs://anderson-JD:9000/spark/abc.txt")

    val fileRDD = file.cache()// 把RDD数据持久化到内存, 相当于MEMORY_ONLY
    file.persist(StorageLevel.MEMORY_ONLY)

    file.flatMap(line => line.split("\t"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("hdfs://anderson-JD:9000/spark/result")
  }
}
