package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-9-16.
  */
object DataFrameOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameOperation")
    val spark = SparkSession
      .builder()
//      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
        .config(conf)
      .getOrCreate()

    // create dataframe
    val df = spark.read.json("hdfs://anderson-JD:9000/examples/people.json")
    df.show()
    // print schema
    df.printSchema()
    // name age
    df.select("name").show()
    //
    df.select(df("name"), df("age") + 1).show()
    // where
    df.filter(df("age") > 21).show()
    // groupby
    df.groupBy("age").count().show()
  }
}
