package org.training.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.training.Contants

/**
  * Created by anderson on 17-10-19.
  * I. 本地启动
  * 1. nc -lp 9999
  * 2. 直接运行当前类
  * 3. 向执行nc的终端输入数据:
2016-10-26-05:05:05,jiayongdianqi,dianshi
2016-10-26-05:05:06,jiayongdianqi,dianshi
2016-10-26-05:05:07,jiayongdianqi,dianshi
2016-10-26-05:05:08,jiayongdianqi,kongtiao
2016-10-26-05:05:09,jiayongdianqi,kongtiao
2016-10-26-05:05:10,jiayongdianqi,bingxiang
2016-10-26-05:05:11,jiayongdianqi,bingxiang
2016-10-26-05:05:12,jiayongdianqi,xiyiji
2016-10-26-05:05:12,tushu,keji
2016-10-26-05:05:12,tushu,keji
2016-10-26-05:05:12,tushu,keji
2016-10-26-05:05:12,tushu,keji
2016-10-26-05:05:12,tushu,renwen
2016-10-26-05:05:12,tushu,renwen
2016-10-26-05:05:12,tushu,shenghuo
2016-10-26-05:05:12,tushu,shenghuo
2016-10-26-05:05:12,tushu,wenyi
2016-10-26-05:05:12,tushu,wenyi
2016-10-26-05:05:12,tushu,jiaoyun
  * 终端输出:
+---------+--------------+-----+
| category|       product|count|
+---------+--------------+-----+
|     keji|         tushu|    4|
| kongtiao| jiayongdianqi|    2|
|bingxiang| jiayongdianqi|    2|
|   xiyiji| jiayongdianqi|    1|
|  dianshi| jiayongdianqi|    3|
|   renwen|         tushu|    2|
|  jiaoyun|         tushu|    1|
|    wenyi|         tushu|    2|
| shenghuo|         tushu|    2|
+---------+--------------+-----+

  * II. 集群执行
  * 1. 打包: cp /home/anderson/GitHub/learn-spark/learn-spark-programming/target/learn-spark-programming-1.0-SNAPSHOT.jar topN.jar
  * 2. 编写启动脚本:
cat topN.sh
/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --class org.training.spark.streaming.TopNOperation \
  --master spark://anderson-JD:7077 \
  --driver-memory 512M \
  --executor-memory 512M \
  --total-executor-cores 2 \
  /home/anderson/gitJD/sparkstreaming/topN.jar
  3. 启动dfs服务:dfs.sh , 启动spark: start-all.sh, 启动hive-metastore:
  4. nc -lp 9999
  5. ./topN.sh
  6. 执行nc命令的终端输入数据:
17/10/19 20:36:45 INFO scheduler.DAGScheduler: Job 20 finished: show at TopNOperation.scala:100, took 0.241691 s
+---------+--------------+-----+
| category|       product|count|
+---------+--------------+-----+
|     keji|         tushu|    4|
| kongtiao| jiayongdianqi|    2|
|bingxiang| jiayongdianqi|    2|
|   xiyiji| jiayongdianqi|    1|
|  dianshi| jiayongdianqi|    3|
|   renwen|         tushu|    2|
|  jiaoyun|         tushu|    1|
|    wenyi|         tushu|    2|
| shenghuo|         tushu|    2|
+---------+--------------+-----+
  */
object TopNOperation {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("TopNOperation")
    val  ssc=new StreamingContext(conf,Seconds(5))
    val fileDS=ssc.socketTextStream(Contants.HOSTNAME, 9999)
    val pairsDS = fileDS.map(log => (log.split(",")(2) + "_ " + log.split(",")(1), 1))
    val pairsCountsDS = pairsDS.reduceByKeyAndWindow((v1: Int, v2: Int) => (v1 + v2), Seconds(60), Seconds(10))
    pairsCountsDS.foreachRDD(categoryproductRDD => {
      val rowRDD = categoryproductRDD.map(tuple => {
        val category = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(category, product, count)
      })

      val structType = StructType(
        Array(
          StructField("category", StringType, true),
          StructField("product", StringType, true),
          StructField("count", IntegerType, true)
        )
      )

      val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
      val categoryProductCountsDF = spark.createDataFrame(rowRDD, structType)
      categoryProductCountsDF.createOrReplaceTempView("product_count")

      val sql = "select category,product,count from (select category,product,count,row_number() over(partition by category order by count desc) rank from product_count) tmp where tmp.rank <= 3"
      val top3DF = spark.sql(sql)
      top3DF.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
