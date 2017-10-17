package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
  * Created by anderson on 17-10-13.
  * 打成jar包 udafdemo.jar
  * 编写启动脚本: udf.sh
  * cat udf.sh
  /home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/bin/spark-submit \
  --driver-class-path /home/anderson/.m2/repository/mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar \
  --class org.training.spark.sql.UDAFDemo \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 1 \
  --files /home/anderson/GitHub/spark-2.2.0-bin-hadoop2.6/conf/hive-site.xml \
  /home/anderson/gitJD/sparksql/udafdemo.jar

  * 注意要启动MetaStore服务!!
  */
object UDAFDemo extends UserDefinedAggregateFunction{

  /**
    * 定义输入的数据类型
    * @return
    */
  override def inputSchema: StructType = StructType(
    StructField("salary", DoubleType, true)::Nil
  )

  /**
    * 定义缓存字段的名称和数据类型
    * @return
    */
  override def bufferSchema: StructType = StructType(
    StructField("total", DoubleType, true)::
    StructField("count", IntegerType, true)::Nil
  )

  /**
    * 定义输出的数据类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 是否指定唯一
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 对参与计算的值进行初始化
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0)
  }

  /**
    * 修改中间状态的值
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val total = buffer.getDouble(0)
    val count = buffer.getInt(1)
    val currentSalary = input.getDouble(0)
    buffer.update(0, total + currentSalary)
    buffer.update(1, count + 1)
  }

  /**
    * 进行全局的统计
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val total1 = buffer1.getDouble(0)
    val count1 = buffer1.getInt(1)
    val total2 = buffer2.getDouble(0)
    val count2 = buffer2.getInt(1)

    buffer1.update(0, total1 + total2)
    buffer1.update(1, count1 + count2)
  }

  /**
    * 最后的目标就是做如下的计算
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    val total = buffer.getDouble(0)
    val count = buffer.getInt(1)
    total / count
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAFDemo")

    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("salary_avg", UDAFDemo)
    spark.sql("select salary_avg(salary) from kyl_test.worker").show()
  }
}
