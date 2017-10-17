package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-9-16.
  */
object DataSourceLoadAndSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD2DataframeProgrammactically")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // 读取json文件
    spark.read.json("person.json")
    // 不指定格式, 默认支持的是parquet
    val df = spark.read.load("examples/src/main/resources/users.parquet")
//    val df = spark.read.parquet("examples/src/main/resources/users.parquet")
    // 通过format指定文件格式
    spark.read.format("json").load("person.json")
    spark.read.format("parquet").load("users.parquet")
    // df保存的时候, 如果没有指定保存的文件格式, 默认是parquet格式
    df.select("name", "age").write.save("nameandage.parquet")
    df.select("name", "age").write.json("nameandage.json")
    df.select("name", "age").write.format("json").save("nameandage.json")
    df.select("name", "age").write.format("parquet").save("nameandage.parquet")

    // 默认模式是SaveMode.ErrorIfExists, 即如果文件已经存在, 则报错
    // SaveMode.Overwrite 覆盖
    // SaveMode.Append 追加
    // SaveMode.Ignore 即如果文件已经存在, 则不进行保存
    df.select("name", "age").write.mode(SaveMode.Append).format("json").save("hdfs://anderson-JD:9000/examples/user.json")
    /**
      * 保存结果文件的时候的策略
      */
  }
}
