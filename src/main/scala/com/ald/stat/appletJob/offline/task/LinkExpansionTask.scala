package com.ald.stat.appletJob.offline.task

import java.util

import com.ald.stat.appletJob.task.TaskTrait
import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.newLink.linkMediaPosition._
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.{SessionBaseImpl, SessionStatImpl}
import com.ald.stat.module.session.newLink._
import com.ald.stat.module.uv.newLink._
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * 外链分析
  * 包括：外链分析 媒体分析  位置分析
  * Created by admin on 2018/5/27.
  */
object LinkExpansionTask extends TaskTrait {

  def allStat_first(dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    ssc: SparkSession, currentTime: Long, hbaseColumn:String, day_wz: String): Unit = {

    hourLinkMediaPositionStat(dateStr, logRecordRdd, kafkaProducer, ssc, currentTime, hbaseColumn, day_wz)
  }


  /**
    * 外链分析 头部汇总 天
    *
    * @param kafkaProducer
    */

  def dailySummaryStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
//    val sessionRDD = DailySummarySessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

//    println("daily sessionRDD -------------------" + sessionRDD.count())
    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
//    val pvAndUvRDD = DailySummaryUVStat.reducePvAndUv_2(uvRDD, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

//    println("daily pvAndUvRDD -------------------" + pvAndUvRDD.count())
    val authUserRDD = DailySummaryAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
//    println("daily authUserRDD -------------------" + authUserRDD.count())

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

//    println("daily ---------------------" + finalRDD.count())

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_link_summary
             |(
             |app_key,
             |day,
             |link_authuser_count,
             |link_visitor_count,
             |link_open_count,
             |link_page_count,
             |link_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$new_auth_user",
             | "$visitor_count",
             | "$open_count",
             | "$total_page_count",
             | "$new_comer_count",
             | "$total_stay_time",
             | "$secondary_avg_stay_time",
             | "$one_page_count",
             | "$bounce_rate",
             | now()
             | )
             | on duplicate key update
             | link_authuser_count="$new_auth_user",
             | link_visitor_count="$visitor_count",
             | link_open_count="$open_count",
             | link_page_count="$total_page_count",
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin

        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }


  /**
    * 外链分析 头部汇总 小时
    *
    * @param kafkaProducer
    */
  def hourSummaryStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                      ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {
    var hour = ""
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"

//    val sessionRDD = HourSummarySessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
//    println("hourly sessionRDD -------------------" + sessionRDD.count())
    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
//    println("hourly pvAndUvRDD -------------------" + pvAndUvRDD.count())
    val authUserRDD = HourSummaryAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
//    println("hourly authUserRDD -------------------" + authUserRDD.count())
    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

//    println("hourly ----------------------" + finalRDD.count())

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_link_summary_hour
             |(
             |app_key,
             |day,
             |hour,
             |link_authuser_count,
             |link_visitor_count,
             |link_open_count,
             |link_page_count,
             |link_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$hour",
             | "$new_auth_user",
             | "$visitor_count",
             | "$open_count",
             | "$total_page_count",
             | "$new_comer_count",
             | "$total_stay_time",
             | "$secondary_avg_stay_time",
             | "$one_page_count",
             | "$bounce_rate",
             | now()
             | )
             | on duplicate key update
             | link_authuser_count="$new_auth_user",
             | link_visitor_count="$visitor_count",
             | link_open_count="$open_count",
             | link_page_count="$total_page_count",
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin
        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }

  /**
    * 外链分析 活动+天
    *
    * @param kafkaProducer
    */
  def dailyLinkStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {

    var hour = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_link_key = ""

//    val sessionRDD = DailyLinkSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailyLinkAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_link
             |(
             |app_key,
             |day,
             |link_key,
             |link_authuser_count,
             |link_visitor_count,
             |link_open_count,
             |link_page_count,
             |link_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$wsr_query_ald_link_key",
             | "$new_auth_user",
             | "$visitor_count",
             | "$open_count",
             | "$total_page_count",
             | "$new_comer_count",
             | "$total_stay_time",
             | "$secondary_avg_stay_time",
             | "$one_page_count",
             | "$bounce_rate",
             | now()
             | )
             | on duplicate key update
             | link_authuser_count="$new_auth_user",
             | link_visitor_count="$visitor_count",
             | link_open_count="$open_count",
             | link_page_count="$total_page_count",
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin

        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }


  /**
    * 外链分析 活动 + 渠道 + 场景值+小时
    *
    * @param dateStr
    * @param logRecordRdd
    */
  def hourLinkMediaPositionStat(dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
                                ssc: SparkSession, currentTime:Long, hbaseColumn:String, day_wz: String): Unit = {

    val tempSessMergeResult = HourLinkMediaPositionSessionStat.statOfflineLink(logRecordRdd, HourLinkMediaPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkMediaPositionSessionStat.doCacheLinkOffline(dateStr, tempSessMergeResult, HourLinkMediaPositionSessionSubDimensionKey)
    sessionRDD.cache()
    //uv
//    val uvRDD = HourLinkMediaPositionUVStat.statIncreaseCacheWithPVLinkOffline(dateStr, HourLinkMediaPositionUidSubDimensionKey, logRecordRdd)
    val uv = HourLinkMediaPositionUVStat.statUVOffline(HourLinkMediaPositionUidSubDimensionKey, logRecordRdd)
    val pv = HourLinkMediaPositionUVStat.statPVOffline(HourLinkMediaPositionUidSubDimensionKey, logRecordRdd)
    val uvRDD = pv.join(uv)
    uvRDD.cache()
       //新增授权
    val resultArray = HourLinkMediaPositionAuthStat.newAuthUserWithOpenIdOffline(dateStr, HourLinkMediaPositionImgSubDimensionKey, logRecordRdd, currentTime, hbaseColumn, day_wz)
    resultArray.cache()
    dailySummaryStat(resultArray, kafkaProducer, ssc, uvRDD, sessionRDD)
    hourSummaryStat(resultArray, kafkaProducer, ssc ,uvRDD, sessionRDD)
    dailyLinkStat(resultArray, kafkaProducer, ssc, uvRDD, sessionRDD)
    hourLinkStat(resultArray, kafkaProducer, ssc, uvRDD, sessionRDD)
    dailyMediaStat(resultArray, kafkaProducer, ssc, uvRDD, sessionRDD)
    hourMediaStat(resultArray, kafkaProducer, ssc, uvRDD, sessionRDD)
    dailyPositionStat(resultArray, kafkaProducer, ssc, uvRDD, sessionRDD)
    hourPositionStat(resultArray, kafkaProducer, ssc, uvRDD, sessionRDD)
  }

  /**
    * 外链分析 活动+小时
    *
    * @param kafkaProducer
    */
  def hourLinkStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                   ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_link_key = ""

//    val sessionRDD = HourLinkSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = HourLinkAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_link
             |(
             |app_key,
             |day,
             |hour,
             |link_key,
             |link_authuser_count,
             |link_visitor_count,
             |link_open_count,
             |link_page_count,
             |link_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$hour",
             | "$wsr_query_ald_link_key",
             | "$new_auth_user",
             | "$visitor_count",
             | "$open_count",
             | "$total_page_count",
             | "$new_comer_count",
             | "$total_stay_time",
             | "$secondary_avg_stay_time",
             | "$one_page_count",
             | "$bounce_rate",
             | now()
             | )
             | on duplicate key update
             | link_authuser_count="$new_auth_user",
             | link_visitor_count="$visitor_count",
             | link_open_count="$open_count",
             | link_page_count="$total_page_count",
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin
        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }


  /**
    * 媒体分析 渠道+天
    *
    * @param kafkaProducer
    */
  def dailyMediaStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                     ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_media_id = ""

//    val sessionRDD = DailyMediaSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailyMediaAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_media
             |(
             |app_key,
             |day,
             |media_id,
             |media_authuser_count,
             |media_visitor_count,
             |media_open_count,
             |media_page_count,
             |media_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$wsr_query_ald_media_id",
             |"$new_auth_user",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |media_authuser_count="$new_auth_user",
             |media_visitor_count="$visitor_count",
             |media_open_count="$open_count",
             |media_page_count="$total_page_count",
             |media_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/media_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/media_open_count,2),0),
             |update_at = now()
          """.stripMargin

        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }


  /**
    * 媒体分析 渠道+小时
    *
    * @param kafkaProducer
    */
  def hourMediaStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {

    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_media_id = ""

//    val sessionRDD = HourMediaSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = HourMediaAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_media
             |(
             |app_key,
             |day,
             |hour,
             |media_id,
             |media_authuser_count,
             |media_visitor_count,
             |media_open_count,
             |media_page_count,
             |media_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$wsr_query_ald_media_id",
             |"$new_auth_user",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |media_authuser_count="$new_auth_user",
             |media_visitor_count="$visitor_count",
             |media_open_count="$open_count",
             |media_page_count="$total_page_count",
             |media_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/media_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/media_open_count,2),0),
             |update_at = now()
          """.stripMargin

        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }

  /**
    * 位置分析 场景值+天
    *
    * @param kafkaProducer
    */
  def dailyPositionStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                        ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = ""

//    val sessionRDD = DailyPositionSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailyPositionAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_position
             |(
             |app_key,
             |day,
             |position_id,
             |position_authuser_count,
             |position_visitor_count,
             |position_open_count,
             |position_page_count,
             |position_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$ag_ald_position_id",
             |"$new_auth_user",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |position_authuser_count="$new_auth_user",
             |position_visitor_count="$visitor_count",
             |position_open_count="$open_count",
             |position_page_count="$total_page_count",
             |position_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/position_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/position_open_count,2),0),
             |update_at=now()
          """.stripMargin

        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }



  /**
    * 位置分析 场景值+小时
    *
    * @param kafkaProducer
    */
  def hourPositionStat(resultArray: RDD[(DimensionKey, (Long, Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       ssc: SparkSession, uvRDD: RDD[(DimensionKey, (Long, Long))],session: RDD[(DimensionKey, SessionSum)]): Unit = {

    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var hour = ""
    var ag_ald_position_id = ""

//    val sessionRDD = HourPositionSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val pvAndUvRDD = uvRDD.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = HourPositionAuthStat.reduceAuth(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        if(hour != "all"){
          hour = splits(2)
        }
        if(wsr_query_ald_link_key != "all"){
          wsr_query_ald_link_key = splits(3)
        }
        if(wsr_query_ald_media_id != "all"){
          wsr_query_ald_media_id = splits(4)
        }
        if(ag_ald_position_id != "all"){
          ag_ald_position_id = splits(5)
        }

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2

        var avg_stay_time = 0f
        if (total_page_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        var secondary_avg_stay_time = 0f
        if (open_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / open_count
        }

        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
//        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
//          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_position
             |(
             |app_key,
             |day,
             |hour,
             |position_id,
             |position_authuser_count,
             |position_visitor_count,
             |position_open_count,
             |position_page_count,
             |position_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$ag_ald_position_id",
             |"$new_auth_user",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |position_authuser_count="$new_auth_user",
             |position_visitor_count="$visitor_count",
             |position_open_count="$open_count",
             |position_page_count="$total_page_count",
             |position_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/position_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/position_open_count,2),0),
             |update_at=now()
          """.stripMargin


        //数据进入kafka
        sendToKafkaWithAK(kafka, sqlInsertOrUpdate,app_key)
      })
    })
  }

  def returnSql(app_key: String, day: String, hour: String, wsr_query_ald_link_key: String, wsr_query_ald_media_id: String,
                ag_ald_position_id: String, new_auth_user: Long, visitor_count: Long, open_count: Long, total_page_count: Long,
                new_comer_count: Long, total_stay_time: Long, secondary_avg_stay_time: Float, one_page_count: Long,
                bounce_rate: Float): String = {
    val sql =
      s"""
         |insert into aldstat_daily_link_monitor
         |(
         |app_key,
         |day,
         |hour,
         |wsr_query_ald_link_key,
         |wsr_query_ald_media_id,
         |ag_ald_position_id,
         |authuser_count,
         |visitor_count,
         |open_count,
         |total_page_count,
         |new_comer_count,
         |total_stay_time,
         |secondary_avg_stay_time,
         |one_page_count,
         |bounce_rate,
         |update_at
         |)
         | values
         | (
         | "$app_key",
         | "$day",
         | "$hour",
         | "$wsr_query_ald_link_key",
         | "$wsr_query_ald_media_id",
         | "$ag_ald_position_id",
         | "$new_auth_user",
         | "$visitor_count",
         | "$open_count",
         | "$total_page_count",
         | "$new_comer_count",
         | "$total_stay_time",
         | "$secondary_avg_stay_time",
         | "$one_page_count",
         | "$bounce_rate",
         | now()
         | )
         | on duplicate key update
         | authuser_count="$new_auth_user",
         | visitor_count="$visitor_count",
         | open_count="$open_count",
         | total_page_count="$total_page_count",
         | new_comer_count="$new_comer_count",
         | total_stay_time="$total_stay_time",
         | secondary_avg_stay_time=ifnull(round(total_stay_time/open_count,2),0),
         | one_page_count="$one_page_count",
         | bounce_rate=ifnull(round(one_page_count/open_count,2),0),
         | update_at = now()
          """.stripMargin
    sql
  }
}
