package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-9-16.
  */

case class Persons(name : String,age : Int)

object RDD2DataframeReflection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD2DataframeReflection")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    // 先创建RDD, 然后调用toDF()方法转换为DataFrame, 调用toDF()方法需要导入隐式转换: import spark.implicits._
    val peopleDF = spark.sparkContext
      .textFile("hdfs://anderson-JD:9000/examples/people.txt")
      .map(_.split(","))
      .map(attributes => Persons(attributes(0), attributes(1).trim.toInt))
        .toDF()

    peopleDF.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")


    teenagersDF.rdd.foreach(row => println(row.getString(0) + " " + row.getString(1)))
    teenagersDF.rdd.saveAsTextFile("hdfs://anderson-JD:9000/examples/RDD2DataframeProgrammactically")

    teenagersDF.map(teenager => "Name:" + teenager(0) + ", age:" + teenager(1)).show()
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  }
}
