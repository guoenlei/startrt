package com.ald.stat.appletJob.offline

import java.util.{Date, Properties}

import com.ald.stat.appletJob.AbstractBaseJob
import com.ald.stat.appletJob.offline.task._
import com.ald.stat.log.LogRecord
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object AnalysisLinkExpansion extends AbstractBaseJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

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
    val logs_1 = ArgsTool.getLogs(ssc, "hdfs://10.0.100.17:4007/ald_log_parquet")
    //    ArgsTool.getLogsDF(ssc, "hdfs://10.0.100.17:4007/stat/utm").createOrReplaceTempView("utm_table")
    //    ssc.read.parquet("hdfs://10.0.100.17:4007/stat/utm/2019041517/*").createOrReplaceTempView("utm_table")//.toJSON.rdd
    //    ssc.read.json("/stat/test/2019-04-25json.txt").createOrReplaceTempView("utm_table")//.toJSON.rdd
    //    val logs_2 = ssc.sql(
    //      """
    //        |SELECT ak,pp,img,city,utm_s as wsr_query_utm_s,
    //        |dr,scene,uu,op,et,ev,ifo,at,v,utm_m as wsr_query_utm_m
    //        |FROM utm_table
    //        |WHERE scene = '1058' or scene = '1035' or scene = '1014' or scene = '1038'
    //      """.stripMargin).toJSON.rdd
    val logs = logs_1 //.union(logs_2)
    /** 业务操作 */

    val dateStr = ComputeTimeUtils.getDateStr(new Date())
    val dateLong = ComputeTimeUtils.tranTimeToLongNext(day_wz)
    val currentTime = (new Date()).getTime - (((new Date()).getTime - dateLong) / 86400000) * 86400000
    val hbaseColumn = "offline"
    val coonMap = DBUtils.getSplitDBTuple()
    val sql = "select app_key,link_key,media_id from ald_link_trace where is_del = 0"
    val linkMap = DBUtils.readQRMapFromMysqlSplit(coonMap, sql)
    val boMap = ssc.sparkContext.broadcast(linkMap)
    val logRecordRdd = logs.mapPartitions(par => {

      val recordsRdd = ArrayBuffer[LogRecord]()
      try {
        par.foreach(line => {
          //            val pair = JSON.parseObject(line)
          val logRecord = LogRecord.line2Bean(line)
          if (logRecord != null &&
            StringUtils.isNotBlank(logRecord.ak) &&
            StringUtils.isNotBlank(logRecord.at) &&
            StringUtils.isNotBlank(logRecord.ev) &&
            StringUtils.isNotBlank(logRecord.uu) &&
            StringUtils.isNotBlank(logRecord.et) &&
            StringUtils.isNotBlank(logRecord.dr) &&
            StrUtils.isInt(logRecord.dr) &&
            StringUtils.isNotBlank(logRecord.pp) &&
            logRecord.pp != "null" &&
            StringUtils.isNotBlank(logRecord.img) &&
            StringUtils.isNotBlank(logRecord.ifo) &&
            StringUtils.isNotBlank(logRecord.scene) && //用scene替换 position_id
            StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
            StringUtils.isNotBlank(logRecord.wsr_query_ald_media_id)
          ) {
            logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正

            val media_id = boMap.value.get(logRecord.ak + ":" + logRecord.wsr_query_ald_link_key.trim)
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

              if (logRecord.v >= "7.0.0") {
                if (logRecord.ev != "app") {
                  recordsRdd += logRecord
                }
              } else {
                recordsRdd += logRecord
              }
            }
          }
        })
      } catch {
        case jce: JedisConnectionException => jce.printStackTrace()
      }
      recordsRdd.iterator
    })
    logRecordRdd.cache()
    //    println("rdd.count =============================" + logRecordRdd.count())
    LinkExpansionTask.allStat_first(dateStr, logRecordRdd, kafkaProducer, ssc, currentTime, hbaseColumn, day_wz)
    boMap.destroy()
    ssc.stop()
  }

}
