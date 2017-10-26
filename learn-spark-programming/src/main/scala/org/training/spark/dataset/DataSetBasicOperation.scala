package org.training.spark.dataset

import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-10-20.
  */
object DataSetBasicOperation {
  case class Student(name: String, age: Long, classID: Long, gender: String, isnew: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DataSetBasicOperation")
      .master("local")
      .getOrCreate()
    //导入隐士转换
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //DataFrame=DataSet[row]
    val studentDF = spark.read.json("/home/anderson/GitHub/learn-spark/learn-spark-programming/src/main/resources/student.json")
    //我们这儿也可以对这个DateSet进行持久化, 如果多次使用这个Dataset那么就选择进行
    //持久化，避免多次重复计算
    studentDF.cache()
    // studentDF.persist(newLevel)
    //view table
    studentDF.createOrReplaceTempView("student")

    spark.sql("select * from student where age > 30").show()

    //可以查询SQL语句的执行计划
    spark.sql("select * from student where age > 30").explain()

    studentDF.printSchema()
    val student30DF = spark.sql("select * from student where age > 30")
//    student30DF.write.parquet("hdfs://anderson-JD:9000/student30DF")

    // 把Dataframe转换称为dataset   untyped ->  typed
    val studentDS = studentDF.as[Student]
    studentDS.show()
    studentDS.printSchema()
    studentDS.toDF()
  }
}
