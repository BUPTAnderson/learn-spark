package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-9-16.
  * spark-shell --master yarn --deploy-mode client
  */
object JsonOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JsonOperation")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    val path = "hdfs://anderson-JD:9000/examples/people.json"
    val peopleDF = spark.read.json(path)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.write.parquet("hdfs://anderson-JD:9000/examples/teenagerNamesDFresult")

    /**
      * 上面演示的是通过加载json数据源创建datafrma
      * 那么我们还可以通过并行化的方式模拟json数据源, 生成dataframe(禁用)
      */
    val list = Array(
      "{\n\t\"name\":\"马六\",\n\t\"age\":18\n}",
      "{\n\t\"name\":\"张三\",\n\t\"age\":19\n}",
      "{\n\t\"name\":\"王五\",\n\t\"age\":20\n}"
    )
    val jsonRDD = spark.sparkContext.parallelize(list, 1);
    spark.read.json(jsonRDD)

    //
    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()
  }
}
