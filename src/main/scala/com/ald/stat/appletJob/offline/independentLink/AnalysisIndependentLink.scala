package com.ald.stat.appletJob.offline.independentLink

import java.util
import java.util.{Date, Properties}

import com.ald.stat.appletJob.AbstractBaseJob
import com.ald.stat.appletJob.offline.independentLink.task._
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object AnalysisIndependentLink extends AbstractBaseJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val newUserHbaseColumn = "newuser"
        val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID_TEST_new"//测试
//    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID"
        val authUserTableName = "STAT.ALDSTAT_ADVERTISE_AUTH_TEST_new"//测试
//    val authUserTableName = "STAT.ALDSTAT_ADVERTISE_AUTH"
    val linkAuthColumn = "authStatus"
    //    val amountAuthColumn = "amountStatus"
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    //    val ssc: SparkContext = new SparkContext(sparkConf)
    val ssc = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //读取parquet
    ArgsTool.analysisArgs(args)
    val day_wz = ArgsTool.day
    val logs = ArgsTool.getDailyDataFrameWithFilter(ssc, "hdfs://10.0.100.17:4007/ald_log_parquet")
    val stream_rdd = logs.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(record => {
        val logRecord = LogRecord.line2Bean(record)
        if (logRecord != null) {
          //          if(logRecord.op != "useless_openid"){
          recordsRdd += logRecord
          //          }
        }
      })
      recordsRdd.iterator
    })

    val dateStr = ComputeTimeUtils.getDateStr(new Date())
    val dateLong = ComputeTimeUtils.tranTimeToLongNext(day_wz)
    //currentTime:存入Hbase的列值，这个时间要取传入的参数day为同一天
    val currentTime = new Date().getTime - ((new Date().getTime - dateLong) / 86400000) * 86400000
    println("currentTime =---------------------------------" + currentTime)
    //    val stream_rdd_2 =  handleNewUserForNewLink(stream_rdd, newUserHbaseColumn, newUserTableName, day_wz, currentTime).cache()

    /** 业务操作 */
    //logRecordRdd_1：用于计算广告监测部分各个指标
    val logRecordRdd_1 = stream_rdd.mapPartitions(par => {
      val linkMap = new util.HashMap[String, String]()
      use(getConnection()) { conn =>
        use(conn.createStatement()) {
          statement =>
            val rs = statement.executeQuery(
              """
                |select app_key,link_key,media_id
                |from ald_link_trace
                |where is_del=0
              """.
                stripMargin)
            while (rs.next()) {
              val link_key = rs.getString(1) + ":" + rs.getString(2)
              val media_id = rs.getInt(3)
              if (link_key != null && media_id != null) {
                linkMap.put(link_key.toString, media_id.toString)
              }
            }
        }
      }
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        //            val pair = JSON.parseObject(line)
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.at) &&
          StringUtils.isNotBlank(logRecord.et) &&
          StringUtils.isNotBlank(logRecord.dr) &&
          StrUtils.isInt(logRecord.dr) &&
          StringUtils.isNotBlank(logRecord.pp) &&
          logRecord.pp != "null" &&
          StringUtils.isNotBlank(logRecord.ifo) &&
          StringUtils.isNotBlank(logRecord.img) &&
          StringUtils.isNotBlank(logRecord.scene) && //用scene替换 position_id
          StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
          StringUtils.isNotBlank(logRecord.wsr_query_ald_media_id)
        ) {
          logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正

          val media_id = linkMap.get(logRecord.ak + ":" + logRecord.wsr_query_ald_link_key.trim)
          if (StringUtils.isNotBlank(media_id)) {
            logRecord.wsr_query_ald_media_id = media_id
            //用场景值替换，position_id
            if (logRecord.scene == "1058" ||
              logRecord.scene == "1035" ||
              logRecord.scene == "1014" ||
              logRecord.scene == "1038") {
              logRecord.wsr_query_ald_position_id = logRecord.scene
            } else {
              logRecord.wsr_query_ald_position_id = "其它"
            }
            recordsRdd += logRecord
          }
        }
      })
      recordsRdd.iterator
    })
    //标记新用户
    val logRecordRdd_link = handleNewUserForNewLinkOffline(logRecordRdd_1, newUserHbaseColumn, newUserTableName, day_wz, currentTime).cache()
    //logRecordRdd_2：用于计算小程序总的访问人数和新用户
    val logRecordRdd_2 = stream_rdd.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.op) &&
          StringUtils.isNotBlank(logRecord.et)
        ) {
          logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
          recordsRdd += logRecord
        }
      })
      recordsRdd.iterator
    })
    val totalCurrentTime = (new Date()).getTime - (((new Date()).getTime - dateLong) / 86400000) * 86400000
    println("totalCurrentTime =---------------------------------" + totalCurrentTime)
    val logRecordRdd_total = handleNewUserForNewLinkOffline(logRecordRdd_2, newUserHbaseColumn, newUserTableName, day_wz, totalCurrentTime).cache()

    println("***********************logRecordRdd_total : Count is " + logRecordRdd_total.count() + "*************************")
    LinkIndependentTask.allStat_first(dateStr, logRecordRdd_link, kafkaProducer, ssc, currentTime, linkAuthColumn, day_wz, logRecordRdd_total, authUserTableName, totalCurrentTime)

    ssc.stop()
  }

}
