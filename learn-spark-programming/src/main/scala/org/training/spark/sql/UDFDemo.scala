package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-10-13.
  * 这里可以直接在spark-shell中执行
  * ./spark-shell --master yarn --deploy-mode client
  * 由于spark-shell已经创建了SparkSession( Spark session available as 'spark'. ), 所以直接通过spark变量创建udf (前提是启动了MetaStore服务, 否则, 如果在$SPARK_HOME/conf/下有hive-site.xml, 则无法创建SparkSession,
  * 如果在$SPARK_HOME/conf/下没有hive-site.xml, 则能购创建SparkSession, 但是该SparkSession是获取不到库表信息的):
  *
  * scala> spark.udf.register("strLen", (str : String) => {
     |       if (str != null) {
     |         str.length
     |       } else {
     |         0
     |       }
     |     })
  * res0: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(StringType)))
  * 创建成功, 直接调用udf : strLen执行spark-sql
  * scala> spark.sql("select strLen(name) from kyl_test.student").show()
    +----------------+
    |UDF:strLen(name)|
    +----------------+
    |               7|
    |               4|
    |               6|
    +----------------+
  * scala>
  */
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDFDemo")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    /**
      * 第一个参数: 自定义函数的函数名
      * 第二个参数: 就是一个匿名函数
      */
    spark.udf.register("strLen", (str : String) => {
      if (str != null) {
        str.length
      } else {
        0
      }
    })

    spark.sql("select strLen(name) from kyl_test.student").show()
  }
}
