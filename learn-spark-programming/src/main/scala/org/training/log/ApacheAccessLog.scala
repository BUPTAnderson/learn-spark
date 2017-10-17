package org.training.log

/**
  * Created by anderson on 17-9-12.
  */
case class ApacheAccessLog(
                          ipAddress : String, // ip地址
                          clientIdentity : String, // 标识符
                          userId : String, //用户ID
                          dataTime : String, // 时间
                          method : String, // 请求方式
                          endPoint : String, // 请求网址
                          protocol : String, // 协议
                          responseCode : Int, // 网页请求响应类型
                          contentSize : Long // 返回内容长度
                          )

object ApacheAccessLog {
  def parseLog(log : String): ApacheAccessLog = {
    val logArray = log.split("#");
    val url = logArray(4).split(" ")
    ApacheAccessLog(logArray(0), logArray(1), logArray(2), logArray(3), url(0), url(1), url(2), logArray(5).toInt, logArray(6).toLong)
  }
}
