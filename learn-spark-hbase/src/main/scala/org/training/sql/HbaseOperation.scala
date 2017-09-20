package org.training.sql

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by anderson on 17-9-20.
  * hbase 表:
  create 'person','f'
  put 'person','1','f:name','zhangxueyou'
  put 'person','1','f:age','100'
  put 'person','2','f:name','liudehua'
  put 'person','2','f:age','110'
  put 'person','3','f:name','guofucheng'
  put 'person','3','f:age','105'

  hbase(main):007:0> scan 'person'
ROW                               COLUMN+CELL
 1                                column=f:age, timestamp=1505877230177, value=100
 1                                column=f:name, timestamp=1505877219600, value=zhangxueyou
 2                                column=f:age, timestamp=1505819209523, value=110
 2                                column=f:name, timestamp=1505819191234, value=liudehua
 3                                column=f:age, timestamp=1505819242350, value=105
 3                                column=f:name, timestamp=1505819225356, value=guofucheng
3 row(s) in 0.1460 seconds

  */
case class Person(id:String, age:String, name:String)

object HbaseOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HbaseOperation")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
//    val sc = new SparkContext(conf)
    import spark.implicits._
    /**
      * conf: Configuration
      * fClass: Class[F], 表的格式
      * fClass: Class[K], 表的格式
      * fClass: Class[V], 表的格式
      *
      * RDD[(K, V)] , 返回值
      * 其实这个地方, 如果有同学用mapreduce操作过base, 那么这跟mapreduce操作hbase是一样的
      */
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "person")
//    hbaseConf.set("hbase.zookeeper.quorum", "anderson-JD:2181")
//    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    val personRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    val count = personRDD.count()
    println("---------------------------------- count: " + count)

    /**
      * 把数据封装成一个DataFrame, 然后注册成一张表, 就可以用sql语句去操作hbase数据库了
      */
    val pRDD = personRDD.map(tuple => {
      val rowKey = Bytes.toString(tuple._1.get())
      val result = tuple._2
      var rowStr = rowKey + ","
      for (cell <- result.rawCells()) {
//        val r = Bytes.toString(CellUtil.cloneRow(cell))
        val q = Bytes.toString(CellUtil.cloneQualifier(cell))
//        val f = Bytes.toString(CellUtil.cloneFamily(cell))
        val v = Bytes.toString(CellUtil.cloneValue(cell))
        rowStr += v + ","
      }
      //1, 110, zhangxueyou
      //2, 110, liudehua
      rowStr.substring(0, rowStr.length - 1)
    })


    val rowPersonRDD = pRDD.map(str => str.split(","))
      .map(row => Person(row(0), row(1), row(2)))

    val personDF = rowPersonRDD.toDF()
    personDF.createOrReplaceTempView("person")
    spark.sql("select id, name from person").show()
  }
}
