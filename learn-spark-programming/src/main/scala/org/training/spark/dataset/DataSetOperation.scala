package org.training.spark.dataset

import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-10-20.
  * 直接运行当前类
  */
object DataSetOperation {
  /**
    *
    * 大家通过观察刚刚的Dataset APi发现，DataSet的api跟RDD的非常类型。
    * DataSet的APi 也分transformation操作和Action的操作。
    * transformation 具有lazy的特性
    * action会触发job的运行
    *
    * Dataset里面也有持久化的操作。RDD的时候也有持久化。
    *
    * 包括我们之前在第二阶段，重点讲解了RDD调优，而那些调优的思路，对这个DataSet一样通用！！
    *
    * DataSet【Untyped transformations】
    *
    *
    * 统计一下，每个班的应届毕业生按性别统计平均年纪
    * 思路：
    * 1） 按班级和性别分组
    * 2）统计平均年纪
    *
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DataSetOperation")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //DataFrame=DataSet[row]
    val studentDF = spark.read.json("/home/anderson/GitHub/learn-spark/learn-spark-programming/src/main/resources/student.json")
    val classDF = spark.read.json("/home/anderson/GitHub/learn-spark/learn-spark-programming/src/main/resources/class.json")

    studentDF.filter("isnew != 'no'")
      .join(classDF, $"classID" === $"id")
      .groupBy(classDF("classname"), studentDF("gender"))
      .agg(avg(studentDF("age")))
      .show()
  }
}
