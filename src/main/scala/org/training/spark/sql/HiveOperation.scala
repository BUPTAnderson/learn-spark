package org.training.spark.sql

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-9-19.
  */
object HiveOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HiveOperation")
//      .set("spark.sql.warehouse.dir", "")

//    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sql("use kyl_test")
    spark.sql("drop table if exists student")
    spark.sql("create table student(name string, age int) row format delimited fields terminated by ',' ")
    spark.sql("LOAD DATA LOCAL INPATH '/home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/examples/src/main/resources/student.txt' INTO TABLE student")
    val teenagesDF = spark.sql("select name, age from student where age >=13 and age <= 19")
//    teenagesDF.createOrReplaceTempView("teenages_student")
    teenagesDF.write.saveAsTable("teenages_student")
  }
}
