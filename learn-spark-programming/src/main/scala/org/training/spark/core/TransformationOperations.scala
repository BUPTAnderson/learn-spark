package org.training.spark.core

import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by anderson on 17-9-8.
  */
object TransformationOperations {
  def filter(): Unit = {
    // sc
    val conf = new SparkConf().setMaster("local").setAppName("filter")
    val sc = new SparkContext(conf)
    val number = Array(1, 2, 3, 4, 5, 6, 7)
    // 1代表分区数, 也可以不传该参数, 默认为1
    val numberRDD = sc.parallelize(number, 1)
    numberRDD.filter(num => num % 2 == 0)
      .foreach(x => println(x))
  }

  def map(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val names = Array("张三", "李四", "王五", "马六")
    val namesRDD = sc.parallelize(names)
    namesRDD.map(name => "hello, " + name)
      .foreach(name => println(name))
  }

  def flatMap(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("flatMap")
    val sc = new SparkContext(conf)
    val words = Array("hello,world", "hello,hadoop", "hello,spark")
    val wordsRDD = sc.parallelize(words)
    wordsRDD.flatMap(word => word.split(","))
      .foreach( word => println(word))
  }

  def groupByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val names = Array(
      Tuple2("北京", "张三"),
      Tuple2("上海", "李四"),
      Tuple2("上海", "王五"),
      Tuple2("北京", "马六"),
      Tuple2("广州", "田七")
    )
    val namesRDD = sc.parallelize(names, 1)
    namesRDD.groupByKey(1).foreach(result => {
      print("城市:" + result._1 + ", 姓名:")
      result._2.foreach(name => print(" " + name))
      println()
    })
  }

  def reduceByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduceByKey")
    val sc = new SparkContext(conf)
    val names = Array(
      Tuple2("张三", 80),
      Tuple2("李四", 90),
      Tuple2("王五", 80),
      Tuple2("张三", 100),
      Tuple2("李四", 60)
    )
    val namesRDD = sc.parallelize(names, 1)
    namesRDD.reduceByKey(_ + _)
      .foreach(result => println(result._1 + "得分:" + result._2))
  }

  def sortByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sortByKey")
    val sc = new SparkContext(conf)
    val names = Array(
      Tuple2(80, "张三"),
      Tuple2(90, "李四"),
      Tuple2(80, "王五"),
      Tuple2(100, "马六"),
      Tuple2(60, "田七")
    )
    val namesRDD = sc.parallelize(names, 1)
    // d第一个参数是升序还是降序, 默认值是true，升序
    namesRDD.sortByKey(false, 1).foreach(
      result => println(result._1 + ", " + result._2)
    )
  }

  def join(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("join")
    val sc = new SparkContext(conf)
    val names = Array(
      new Tuple2[Integer, String](1, "张三"),
      new Tuple2[Integer, String](2, "李四"),
      new Tuple2[Integer, String](3, "王五"),
      new Tuple2[Integer, String](4, "马六"),
      new Tuple2[Integer, String](5, "田七"),
      new Tuple2[Integer, String](6, "朱八"),
      new Tuple2[Integer, String](7, "赵九"),
      new Tuple2[Integer, String](8, "钱十")
    )
    val scores = Array(
      new Tuple2[Integer, Integer](1, 100),
      new Tuple2[Integer, Integer](2, 90),
      new Tuple2[Integer, Integer](3, 80),
      new Tuple2[Integer, Integer](6, 70),
      new Tuple2[Integer, Integer](4, 95),
      new Tuple2[Integer, Integer](5, 98),
      new Tuple2[Integer, Integer](8, 68),
      new Tuple2[Integer, Integer](7, 70)
    )
    val namesRDD = sc.parallelize(names, 1)
    val scoresRDD = sc.parallelize(scores, 1)
    namesRDD.join(scoresRDD).foreach(
      result => println("学号:" + result._1 + ", 姓名:" + result._2._1 + ", 分数:" + result._2._2)
    )
  }

  def cogroup(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("cogroup")
    val sc = new SparkContext(conf)
    val names = Array(
      new Tuple2[Integer, String](1, "张三"),
      new Tuple2[Integer, String](2, "李四"),
      new Tuple2[Integer, String](3, "王五"),
      new Tuple2[Integer, String](1, "马六"),
      new Tuple2[Integer, String](2, "田七"),
      new Tuple2[Integer, String](3, "朱八")
    )
    val scores = Array(
      new Tuple2[Integer, Integer](1, 100),
      new Tuple2[Integer, Integer](2, 90),
      new Tuple2[Integer, Integer](3, 100),
      new Tuple2[Integer, Integer](3, 99),
      new Tuple2[Integer, Integer](2, 95),
      new Tuple2[Integer, Integer](2, 98),
      new Tuple2[Integer, Integer](1, 98),
      new Tuple2[Integer, Integer](3, 95)
    )
    val namesRDD = sc.parallelize(names, 1)
    val scoresRDD = sc.parallelize(scores, 1)
    namesRDD.cogroup(scoresRDD)
      .foreach(result => {
        print("班级: " + result._1 + ", 班长:")
        result._2._1.foreach(name => print("　" +name))
        print(", 最高分:")
        result._2._2.foreach(score => print(" " + score))
        println()
      })
  }

  def intersection(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("intersection")
    val sc = new SparkContext(conf)
    val lista = Array(1, 2, 3, 4, 5)
    val listb = Array(3, 5, 6, 7, 9)
    val listaRDD = sc.parallelize(lista)
    val listbRDD = sc.parallelize(listb)
    listaRDD.intersection(listbRDD).foreach(
      value => println(value)
    )
  }

  // 求并集但是不去重
  def union(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("union")
    val sc = new SparkContext(conf)
    val lista = Array(1, 2, 3, 4, 5)
    val listb = Array(3, 5, 6, 7, 9)
    val listaRDD = sc.parallelize(lista)
    val listbRDD = sc.parallelize(listb)
    listaRDD.union(listbRDD).foreach(
      value => println(value)
    )
  }

  // 去重
  def distinct(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("distinct")
    val sc = new SparkContext(conf)
    val list = Array(1, 2, 3, 4, 5, 3, 5, 6, 7, 9)
    val listRDD = sc.parallelize(list)
    listRDD.distinct().foreach(
      value => println(value)
    )
  }

  // 笛卡尔积
  def cartesian(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("cartesian")
    val sc = new SparkContext(conf)
    val lista = Array(1, 2, 3)
    val listb = Array("张三", "李四", "王五")
    val listaRDD = sc.parallelize(lista)
    val listbRDD = sc.parallelize(listb)
    listaRDD.cartesian(listbRDD).foreach(
      result => {
        println(result._1 + " -> " + result._2)
      }
    )
  }

  def mapPartitions(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("mapPartitions")
    val sc = new SparkContext(conf)
    val lista = Array(1, 2, 3, 4, 4, 5, 6, 6, 7)
    val listaRDD = sc.parallelize(lista)
    // repartition, 场景: rdd => filter => repartition
    listaRDD.repartition(2)
    // 上面这个语句相当于下面的语句, coalesce方法中的true是默认值, 只进行shuffle操作
//    listaRDD.coalesce(2, true)
    // number就是一个分区的数据
    listaRDD.mapPartitions(number => number.map(
      x => "hello, " + x
    ), true)
      .foreach(result => println(result))
  }

  // 采样
  def sample(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    val poker = Array("红桃A", "红桃2", "红桃3", "红桃4", "红桃5", "红桃6", "红桃7", "红桃8", "红桃9", "红桃10", "红桃J", "红桃Q", "红桃K")
    val pokerRDD = sc.parallelize(poker)
    // 第一个参数指是否放回, 第二个值是采样比例
    pokerRDD.sample(true, 0.1)
      .foreach(result => println("--------------------------" + result))
  }

  def aggregateByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("aggregateByKey")
    val sc = new SparkContext(conf)
    val words = Array("hello,world", "hello,hadoop", "hello,spark")
    val wordsRDD = sc.parallelize(words)
    wordsRDD.flatMap( x => x.split(","))
      .map(word => (word, 1))
      // 第一个参数是初始值, 第二个参数是一个函数, 类似于mapreduce里面的combine操作(在Map端做reduce的操作), 第三个参数也是一个函数, 全局汇总, 就是一个reduce的操作
      .aggregateByKey(0)(
      (a : Int, b : Int) => a + b,
      (a : Int, b : Int) => a + b)
      .foreach(result => println(result._1 + " " + result._2))
  }

  /**
    * 假设partition 1: 1, 2, 3, 4
    * partition 2: 5, 6, 7, 8
    * new map 长度可变的一个map集合
    * map(0_1, 1)
    * map(0_2, 2)
    * map(0_3, 3)
    * map(0_4, 4)
    * map(1_5, 5)
    * map(1_6, 6)
    * map(1_7, 7)
    * map(1_8, 8)
    */
  def mapPartitionsWithIndex(): Unit = {
    // sc
    val conf = new SparkConf().setMaster("local").setAppName("mapPartitionsWithIndex")
    val sc = new SparkContext(conf)
    val number = Array(1, 2, 3, 4, 5, 6, 7, 8)
    val numberRDD = sc.parallelize(number, 2)
    numberRDD.mapPartitionsWithIndex((x, iter) => {
      import scala.collection.mutable.Map
      val map: Map[String, Int] = Map()
      while (iter.hasNext) {
        val i = iter.next()
        map += (x + "_" + i -> i)
      }
      map.iterator // 返回一个Iterator
    }, true)
      .foreach(result => println(result))
  }

  def repartitionAndSortWithinPartitions(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("repartitionAndSortWithinPartitions")
    val sc = new SparkContext(conf)
    val number = Array(1, 2, 3, 4, 5, 6, 7, 8)
    val numberRDD = sc.parallelize(number)
    numberRDD.map(x => (x, new util.Random().nextInt(9)))
      .repartitionAndSortWithinPartitions(new MyPartition(2))
      .foreach(result => println(result._1 + " " + result._2))
  }

  class MyPartition(partitions : Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
     key.toString.hashCode % 2
    }
  }

  def main(args: Array[String]): Unit = {
//    filter()
//    map()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
//    join()
//    cogroup()
//    intersection()
//    union()
//    distinct()
//    cartesian()
//    mapPartitions() // repartition // coalesce
//    sample()
//    aggregateByKey()
//    mapPartitionsWithIndex()
    repartitionAndSortWithinPartitions()
  }
}
