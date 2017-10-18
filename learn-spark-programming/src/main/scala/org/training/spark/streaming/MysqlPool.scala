package org.training.spark.streaming

import java.sql.Connection
import java.sql.DriverManager

/**
  * Created by anderson on 17-10-17.
  */
object MysqlPool {
  private val max = 8 //连接池的连接总数
  private val connectionNum = 10 //每次产生的连接数
  private var conNum = 0 //当前连接池已经产生的连接数

  import java.util
  private val pool=new util.LinkedList[Connection]() //连接池

  {

    Class.forName("com.mysql.jdbc.Driver")
  }

  /**
    * 释放连接
    */
  def releaseConn(conn:Connection): Unit = {
//    val success = pool.remove(conn)
//    if (success) {
//      conn.close()
//    }
    pool.push(conn)
  }

  /**
    * 获取连接
    */
  def getJdbcConnect(): Connection = {
    //同步代码块
    AnyRef.synchronized({
      if (pool.isEmpty()) {
        for ( i <- 1 to connectionNum) {
          val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
          pool.push(conn)
          conNum + 1
        }
      }

      pool.poll()
    })
  }
}
