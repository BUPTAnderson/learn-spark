package org.training.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-9-16.
  * 在spark-env.sh中设置了:
  * export SPARK_CLASSPATH=$SPARK_CLASSPATH:/home/anderson/.m2/repository/mysql/mysql-connector-java/5.1.35/mysql-connector-java-5.1.35.jar
  * 则./spark-submit 不能使用--driver-class-path
  * 如果没有配置 则需要在启动命令中指定 --driver-class-path
  *  cat jdbcoperation.sh
  /home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --driver-class-path /home/anderson/.m2/repository/mysql/mysql-connector-java/5.1.35/mysql-connector-java-5.1.35.jar \
  --class org.training.spark.sql.JDBSOperation \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 1 \
  /home/anderson/gitJD/sparksql/jdbcoperation.jar \

  */
object JDBCOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBSOperation")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    val studentDF = spark.read.jdbc("jdbc:mysql://localhost:3306/test", "student", properties)
    studentDF.show()
  }
}
