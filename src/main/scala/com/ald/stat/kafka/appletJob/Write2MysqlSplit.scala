package com.ald.stat.kafka.appletJob

import java.sql.Connection

import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{ConfigUtils, DBUtils, MysqlUtil}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Write2MysqlSplit {

  val logger = LoggerFactory.getLogger("write2mysql wait 120 ms")

  def main(args: Array[String]): Unit = {
    //  创建SparkConf

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    //          .setMaster(ConfigUtils.getProperty("spark.master.host"))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))

//    val group = "KAFKA2HBASE_SQL"
    val group = ConfigUtils.getProperty("kafka.mysql.split.sql.group.id")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = ConfigUtils.getProperty("kafka.mysql.split.sql.topic")
    logger.info("topic:" + topic.toString)
    val topics = Array(topic)
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //广播连接信息map(ak,连接信息)


    dStream.foreachRDD(rdd => {

      val boConn = ssc.sparkContext.broadcast(DBUtils.getSplitDBTuple())

      rdd.foreachPartition(par => {
        var defaultWXConnection:Connection =  null
        var defaultQXConnection:Connection =  null
        val aksConnection = new mutable.HashMap[String,Connection]() //key是数据库名加上平台
        boConn.value.foreach(akconn=>{
          try {
            defaultWXConnection = MysqlUtil.getConnection()
            defaultQXConnection = MysqlUtil.getQQConnection()
            val url = "jdbc:mysql://" + akconn._2._3 + ":" + akconn._2._4 + "/" + akconn._2._2 + "?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
            val key = akconn._2._2+akconn._2._1
            val connection = MysqlUtil.getOtherConnection(key, url, akconn._2._5, akconn._2._6)
            if(!aksConnection.contains(key)){
              aksConnection.put(key,connection)
            }
          }
          catch {
            case t:Throwable=>{
              println(" split db get Connection error:"+ akconn)
              t.printStackTrace()
            }
          }
        })
        try {

          par.foreach(line => {
            val akAndTe = line.key()
            var connection: Connection = null
            if (akAndTe.split(":").length == 2) { //区分微信和QQ进行入库
              val ak = akAndTe.split(":")(0)
              val te = akAndTe.split(":")(1)

              if (boConn.value.contains(ak)) {
                val tuple = boConn.value(ak)
                connection = aksConnection(tuple._2 + tuple._1)
              }
              else {
                if (te == "qx") {
                  connection = defaultQXConnection
                }
                else {
                  connection = defaultWXConnection
                }
              }
            }
            else {
              connection = defaultWXConnection
            }
            try {

              use(connection.createStatement()){statement=>statement.executeUpdate(line.value)}
              //               e     stmt.addBatch(line.value())
            } catch {
              case t: Throwable => {
                logger.error("save to db error!!!!!! stmt sql.............." + line, t)
              }
            }
          })
        }
        finally {
          // close connection
          DBUtils.closeConn(defaultWXConnection)
          DBUtils.closeConn(defaultQXConnection)
          aksConnection.values.foreach(c=>DBUtils.closeConn(c))
        }
      })
      boConn.destroy()
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
