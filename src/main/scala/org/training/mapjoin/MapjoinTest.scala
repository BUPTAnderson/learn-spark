package org.training.mapjoin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by anderson on 17-9-14.
  */
object MapjoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MapjoinTest")
    val sc = new SparkContext(conf)
    val lista = Array(
      Tuple2[String, String]("001", "令狐冲"),
      Tuple2[String, String]("002", "任盈盈")
    )
    val listb = Array(
      Tuple2[String, String]("001", "一班"),
      Tuple2[String, String]("002", "二班")
    )
    val listaRDD = sc.parallelize(lista)
    val listbRDD = sc.parallelize(listb)
    val listadata = listaRDD.collect()
    val listabroadcast = sc.broadcast(listadata)
    listbRDD.map(tuple => {
      import scala.collection.mutable.Map;
      val rdd1map : Map[String, String] = Map()
      for (t <- listabroadcast.value) {
        rdd1map += (t._1 -> t._2)
      }
      // get rddb key value
      val key = tuple._1
      val value = tuple._2
      val rdd1value = rdd1map.get(key).get
      // return
      (key, Tuple2(value, rdd1value))
    })
      .foreach(result => println(result._1 + " " + result._2._1 + " " + result._2._2))
  }
}
