package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
  * Created by anderson on 17-9-16.
  */
object RDD2DataframeProgrammactically {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD2DataframeProgrammactically")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // Create an RDD
    val peopleRDD = spark.sparkContext
      .textFile("hdfs://anderson-JD:9000/examples/people.txt", 1)

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = peopleRDD.map(_.split(","))
      .map(attributes=> Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name, age FROM people where age > 13 and age <= 19")

//    results.rdd.foreach(row => println(row(0) + " " + row(1)))
    results.rdd.foreach(row => println(row.getString(0) + " " + row.getString(1)))
    results.rdd.saveAsTextFile("hdfs://anderson-JD:9000/examples/RDD2DataframeProgrammactically")
  }
}
