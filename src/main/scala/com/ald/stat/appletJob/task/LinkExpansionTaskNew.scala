package com.ald.stat.appletJob.offline.task

import java.util

import com.ald.stat.appletJob.task.TaskTrait
import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.newLink.link.{DailyLinkSessionSubDimensionKey, DailyLinkUidSubDimensionKey, HourLinkSessionSubDimensionKey, HourLinkUidSubDimensionKey}
import com.ald.stat.component.dimension.newLink.linkMedia.{DailyLinkMediaUidSubDimensionKey, HourLinkMediaUidSubDimensionKey}
import com.ald.stat.component.dimension.newLink.linkMediaPosition._
import com.ald.stat.component.dimension.newLink.linkPosition.{DailyLinkPositionUidSubDimensionKey, HourLinkPositionUidSubDimensionKey}
import com.ald.stat.component.dimension.newLink.media.{DailyMediaSessionSubDimensionKey, DailyMediaUidSubDimensionKey, HourMediaSessionSubDimensionKey, HourMediaUidSubDimensionKey}
import com.ald.stat.component.dimension.newLink.mediaPosition.{DailyMediaPositionUidSubDimensionKey, HourMediaPositionUidSubDimensionKey}
import com.ald.stat.component.dimension.newLink.position.{DailyPositionSessionSubDimensionKey, DailyPositionUidSubDimensionKey, HourPositionSessionSubDimensionKey, HourPositionUidSubDimensionKey}
import com.ald.stat.component.dimension.newLink.summary.{DailySessionSubDimensionKey, DailyUidSubDimensionKey, HourSessionSubDimensionKey, HourUidSubDimensionKey}
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.session.newLink._
import com.ald.stat.module.uv.newLink._
import com.ald.stat.utils.KafkaSink
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * 外链分析
  * 包括：外链分析 媒体分析  位置分析
  * Created by admin on 2018/5/27.
  */
object LinkExpansionTaskNew extends TaskTrait {

  def allStat_first(baseRedisKey: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_kafkaProducer: Broadcast[KafkaSink[String, String]], grey_map: util.HashMap[String, String],
                    isOnLine: Boolean, redisPrefix: Broadcast[String], ssc:StreamingContext): Unit = {

    hourLinkMediaPositionStat(dateStr, logRecordRdd, kafkaProducer, ssc, baseRedisKey, redisPrefix)
  }


  /**
    * 外链分析 头部汇总 天
    *
    * @param kafkaProducer
    */

  def dailySummaryStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    val tempSessMergeResult = DailySummarySessionStat.stat(logRecordRdd, DailySessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailySummarySessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailySessionSubDimensionKey, redisPrefix)

//    val sessionRDD = DailySummarySessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = DailySummaryUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailySummaryAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id ,redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)


    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)

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
  def hourSummaryStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                      ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = ""
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"

    val tempSessMergeResult = HourSummarySessionStat.stat(logRecordRdd, HourSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourSummarySessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourSessionSubDimensionKey, redisPrefix)
    //    val sessionRDD = HourSummarySessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
//    println("hourly sessionRDD -------------------" + sessionRDD.count())
    val pvAndUvRDD = HourSummaryUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, HourUidSubDimensionKey, logRecordRdd,redisPrefix)
//    println("hourly pvAndUvRDD -------------------" + pvAndUvRDD.count())
    val authUserRDD = HourSummaryAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
//    println("hourly authUserRDD -------------------" + authUserRDD.count())
    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

//    println("hourly ----------------------" + finalRDD.count())

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        hour = splits(2)

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
  def dailyLinkStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_link_key = ""

//    val sessionRDD = DailyLinkSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    val tempSessMergeResult = DailyLinkSessionStat.stat(logRecordRdd, DailyLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyLinkSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailyLinkSessionSubDimensionKey, redisPrefix)

    val pvAndUvRDD = DailyLinkUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyLinkUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailyLinkAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_link_key = splits(2)

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
    * 外链分析 活动 + 渠道 + 场景值+天
    *
    * @param kafkaProducer
    */
  def dailyLinkMediaPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                                 ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = ""
    var wsr_query_ald_media_id = ""
    var ag_ald_position_id = ""

    val sessionRDD = DailyLinkMediaPositionSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = DailyLinkMediaPositionUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyLinkMediaPositionUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailyLinkMediaPositionAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_link_key = splits(2)
        wsr_query_ald_media_id = splits(3)
        ag_ald_position_id = splits(4)

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
        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 外链分析 活动 + 渠道 + 场景值+小时
    *
    * @param dateStr
    * @param logRecordRdd
    */
  def hourLinkMediaPositionStat(dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], ssc: StreamingContext,
                                baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourLinkMediaPositionSessionStat.stat(logRecordRdd, HourLinkMediaPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkMediaPositionSessionStat.doCacheLink(baseRedisKey, dateStr, tempSessMergeResult, HourLinkMediaPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourLinkMediaPositionUVStat.statIncreaseCacheWithPVLink(baseRedisKey, dateStr, HourLinkMediaPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = HourLinkMediaPositionAuthStat.newAuthUserWithOpenId(baseRedisKey, dateStr, HourLinkMediaPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    val resultArray =finalRDD.repartition(1).mapPartitions(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.map(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = splits(3)
        val wsr_query_ald_media_id = splits(4)
        val ag_ald_position_id = splits(5)

        val sessionSum = row._2._1._2._1
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1._1
        val new_auth_user = row._2._2._1._2
//        (DimensionKey, ((((Long, Long), (Long, Long)), (SessionSum, SessionSum)), ((Long, Long), (Long, Long))))
        //本批次累加的结果，未与redis中的值进行累加的
        val total_page_count_2 = row._2._1._1._2._1
        val visitor_count_2 = row._2._1._1._2._2
        val sessionSum_2 = row._2._1._2._2
        val new_auth_user_2 = row._2._2._2._2

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
//          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count,
//          bounce_rate)

        //数据进入kafka
//        sendToKafka(kafka, sqlInsertOrUpdate)
        (row._1, (((total_page_count_2, visitor_count_2), sessionSum_2), new_auth_user_2))
      })
    })
      .collect() // 拉取数据到driver，action算子，效率低下，易OOM。

    dailySummaryStat(resultArray, kafkaProducer, ssc, logRecordRdd ,dateStr, baseRedisKey, redisPrefix)
    hourSummaryStat(resultArray, kafkaProducer, ssc ,logRecordRdd,dateStr, baseRedisKey, redisPrefix)
    dailyLinkStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
    hourLinkStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
//    dailyLinkMediaPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
    dailyMediaStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
    hourMediaStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
//    dailyMediaPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
//    hourMediaPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
//    dailyLinkMediaStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
//    hourLinkMediaStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
    dailyPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
    hourPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
//    dailyLinkPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
//    hourLinkPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd,dateStr, baseRedisKey, redisPrefix)
  }

  /**
    * 外链分析 活动+小时
    *
    * @param kafkaProducer
    */
  def hourLinkStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                   ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_link_key = ""

//    val sessionRDD = HourLinkSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    val tempSessMergeResult = HourLinkSessionStat.stat(logRecordRdd, HourLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourLinkSessionSubDimensionKey, redisPrefix)

    val pvAndUvRDD = HourLinkUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, HourLinkUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = HourLinkAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        hour = splits(2)
        wsr_query_ald_link_key = splits(3)

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
  def dailyMediaStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                     ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_media_id = ""

//    val sessionRDD = DailyMediaSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    val tempSessMergeResult = DailyMediaSessionStat.stat(logRecordRdd, DailyMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyMediaSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailyMediaSessionSubDimensionKey, redisPrefix)

    val pvAndUvRDD = DailyMediaUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyMediaUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailyMediaAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_media_id = splits(2)

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
    * 媒体分析 渠道+ 场景值+天
    *
    * @param kafkaProducer
    */
  def dailyMediaPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                             ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = ""
    var ag_ald_position_id = ""

    val sessionRDD = DailyMediaPositionSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = DailyMediaPositionUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyMediaPositionUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailyMediaPositionAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_media_id = splits(2)
        ag_ald_position_id = splits(3)

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
        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 媒体分析 渠道+ 场景值+小时
    *
    * @param kafkaProducer
    */
  def hourMediaPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                            ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var wsr_query_ald_link_key = "all"
    var hour = ""
    var wsr_query_ald_media_id = ""
    var ag_ald_position_id = ""

    val sessionRDD = HourMediaPositionSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = HourMediaPositionUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, HourMediaPositionUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = HourMediaPositionAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        hour = splits(2)
        wsr_query_ald_media_id = splits(3)
        ag_ald_position_id = splits(4)

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
        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 媒体分析 活动+渠道+天
    *
    * @param kafkaProducer
    */
  def dailyLinkMediaStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                         ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_link_key = ""
    var wsr_query_ald_media_id = ""

    val sessionRDD = DailyLinkMediaSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = DailyLinkMediaUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyLinkMediaUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailyLinkMediaAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_link_key = splits(2)
        wsr_query_ald_media_id = splits(3)

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
        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 媒体分析 活动+渠道+小时
    *
    * @param kafkaProducer
    */
  def hourLinkMediaStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                        ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_link_key = ""
    var wsr_query_ald_media_id = ""

    val sessionRDD = HourLinkMediaSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = HourLinkMediaUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, HourLinkMediaUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = HourLinkMediaAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        hour = splits(2)
        wsr_query_ald_link_key = splits(3)
        wsr_query_ald_media_id = splits(4)

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
        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 媒体分析 渠道+小时
    *
    * @param kafkaProducer
    */
  def hourMediaStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_media_id = ""

//    val sessionRDD = HourMediaSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    val tempSessMergeResult = HourMediaSessionStat.stat(logRecordRdd, HourMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourMediaSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourMediaSessionSubDimensionKey, redisPrefix)

    val pvAndUvRDD = HourMediaUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, HourMediaUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = HourMediaAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        hour = splits(2)
        wsr_query_ald_media_id = splits(3)

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
  def dailyPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                        ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = ""

//    val sessionRDD = DailyPositionSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    val tempSessMergeResult = DailyPositionSessionStat.stat(logRecordRdd, DailyPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyPositionSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailyPositionSessionSubDimensionKey, redisPrefix)

    val pvAndUvRDD = DailyPositionUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyPositionUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailyPositionAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        ag_ald_position_id = splits(2)

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
    * 位置分析 活动+场景值+天
    *
    * @param kafkaProducer
    */
  def dailyLinkPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                            ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var hour = "all"
    var wsr_query_ald_media_id = "all"
    var wsr_query_ald_link_key = ""
    var ag_ald_position_id = ""

    val sessionRDD = DailyLinkPositionSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = DailyLinkPositionUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, DailyLinkPositionUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = DailyLinkPositionAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_link_key = splits(2)
        ag_ald_position_id = splits(3)

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
        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 位置分析 活动+场景值+小时
    *
    * @param kafkaProducer
    */
  def hourLinkPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                           ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var wsr_query_ald_media_id = "all"
    var hour = ""
    var wsr_query_ald_link_key = ""
    var ag_ald_position_id = ""

    val sessionRDD = HourLinkPositionSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val pvAndUvRDD = HourLinkPositionUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, HourLinkPositionUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = HourLinkPositionAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        hour = splits(2)
        wsr_query_ald_link_key = splits(3)
        ag_ald_position_id = splits(4)

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
        val sqlInsertOrUpdate = returnSql(app_key, day, hour, wsr_query_ald_link_key, wsr_query_ald_media_id,
          ag_ald_position_id, new_auth_user, visitor_count, open_count, total_page_count,
          new_comer_count, total_stay_time, secondary_avg_stay_time, one_page_count, bounce_rate)

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 位置分析 场景值+小时
    *
    * @param kafkaProducer
    */
  def hourPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       ssc: StreamingContext, logRecordRdd: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): Unit = {

    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var hour = ""
    var ag_ald_position_id = ""

//    val sessionRDD = HourPositionSessionStat.reduceSessionOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
  val tempSessMergeResult = HourPositionSessionStat.stat(logRecordRdd, HourPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourPositionSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourPositionSessionSubDimensionKey, redisPrefix)

    val pvAndUvRDD = HourPositionUVStat.statIncreaseCacheWithPVOnline(baseRedisKey, dateStr, HourPositionUidSubDimensionKey, logRecordRdd,redisPrefix)

    val authUserRDD = HourPositionAuthStat.reduceAuthOnline(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        hour = splits(2)
        ag_ald_position_id = splits(3)

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
