package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-10-13.
  */
object WindowFunctionOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowFunctionOperation")

    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use kyl_test")
    spark.sql("drop table IF EXISTS result")
    spark.sql("create table result(ipaddress string, category string, product string, count int)row format delimited fields terminated by ','")
    spark.sql("load data local inpath '/home/anderson/GitHub/result.txt' into table result")
    val top3df = spark.sql("""
        select ipaddress, category, product, count from
        (
        select ipaddress, category, product, count, row_number() over (partition by category order by count desc) rank from result
        ) tmp_result where tmp_result.rank <= 3
      """)
//    spark.sql("drop table IF EXISTS top2_result")
    // 将dateframe保存成表, 采用下面的方式, 如果表已经存在的话会报错
//    top3df.write.saveAsTable("top2_result")
    // 如果表已经存在, 下面的方式会追加到表里, 不会报错, 如果表不存在, 会创建表
    top3df.write.mode(SaveMode.Append).saveAsTable("top2_result")
    // 如果表已经存在, 下面的方式会追加到表里, 如果表不存在, 会报错
//    top3df.write.insertInto("top2_result")
    // 如果表已经存在, 下面的方式会覆盖表里的数据, 如果表不存在, 会报错
//    top3df.write.mode(SaveMode.Overwrite).insertInto("top2_result")
  }
}
