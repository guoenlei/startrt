package com.ald.stat.appletJob.task

import java.util

import com.ald.stat.component.dimension.newLink.link._
import com.ald.stat.component.dimension.newLink.linkMedia._
import com.ald.stat.component.dimension.newLink.linkPosition._
import com.ald.stat.component.dimension.newLink.linkMediaPosition._
import com.ald.stat.component.dimension.newLink.media._
import com.ald.stat.component.dimension.newLink.mediaPosition._
import com.ald.stat.component.dimension.newLink.position._
import com.ald.stat.component.dimension.newLink.summary._
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.session.newLink._
import com.ald.stat.module.uv.newLink._
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 外链分析
  * 包括：外链分析 媒体分析  位置分析
  * Created by admin on 2018/5/27.
  */
object LinkAllTaskNew extends TaskTrait {

  def allStat_first(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                    kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    dailySummaryStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourSummaryStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    dailyLinkStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourLinkStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    dailyLinkMediaPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourLinkMediaPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

  }

  def allStat_second(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                     kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                     grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    dailyMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    dailyMediaPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourMediaPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    dailyLinkMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourLinkMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    dailyPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    dailyLinkPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

    hourLinkPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grey_map, isOnLine, redisPrefix)

  }

  /**
    * 外链分析 头部汇总 天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailySummaryStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                       kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                       grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailySummarySessionStat.stat(logRecordRdd, DailySessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailySummarySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailySessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailySummaryUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = DailySummaryAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 外链分析 头部汇总 小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourSummaryStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                      kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                      grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourSummarySessionStat.stat(logRecordRdd, HourSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourSummarySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourSummaryUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = HourSummaryAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 外链分析 活动+天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyLinkStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                    kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailyLinkSessionStat.stat(logRecordRdd, DailyLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyLinkSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyLinkSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailyLinkUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyLinkUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = DailyLinkAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyLinkImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = splits(2)
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 外链分析 活动 + 渠道 + 场景值+天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyLinkMediaPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                                 kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                                 grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailyLinkMediaPositionSessionStat.stat(logRecordRdd, DailyLinkMediaPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyLinkMediaPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyLinkMediaPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailyLinkMediaPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyLinkMediaPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = DailyLinkMediaPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyLinkMediaPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = splits(2)
        val wsr_query_ald_media_id = splits(3)
        val ag_ald_position_id = splits(4)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 外链分析 活动 + 渠道 + 场景值+小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourLinkMediaPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                                kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                                grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourLinkMediaPositionSessionStat.stat(logRecordRdd, HourLinkMediaPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkMediaPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourLinkMediaPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourLinkMediaPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourLinkMediaPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = HourLinkMediaPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourLinkMediaPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = splits(3)
        val wsr_query_ald_media_id = splits(4)
        val ag_ald_position_id = splits(5)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 外链分析 活动+小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourLinkStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                   kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                   grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourLinkSessionStat.stat(logRecordRdd, HourLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourLinkSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourLinkUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourLinkUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = HourLinkAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourLinkImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = splits(3)
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }


  /**
    * 媒体分析 渠道+天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                     kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                     grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailyMediaSessionStat.stat(logRecordRdd, DailyMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyMediaSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailyMediaUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyMediaUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = DailyMediaAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyMediaImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = splits(2)
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 媒体分析 渠道+ 场景值+天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyMediaPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                             kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                             grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailyMediaPositionSessionStat.stat(logRecordRdd, DailyMediaPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyMediaPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyMediaPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailyMediaPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyMediaPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = DailyMediaPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyMediaPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = splits(2)
        val ag_ald_position_id = splits(3)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 媒体分析 渠道+ 场景值+小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourMediaPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                            kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                            grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourMediaPositionSessionStat.stat(logRecordRdd, HourMediaPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourMediaPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourMediaPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourMediaPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourMediaPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = HourMediaPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourMediaPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = splits(3)
        val ag_ald_position_id = splits(4)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 媒体分析 活动+渠道+天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyLinkMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                         kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                         grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailyLinkMediaSessionStat.stat(logRecordRdd, DailyLinkMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyLinkMediaSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyLinkMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailyLinkMediaUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyLinkMediaUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = DailyLinkMediaAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyLinkMediaImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = splits(2)
        val wsr_query_ald_media_id = splits(3)
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 媒体分析 活动+渠道+小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourLinkMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                        kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                        grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourLinkMediaSessionStat.stat(logRecordRdd, HourLinkMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkMediaSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourLinkMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourLinkMediaUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourLinkMediaUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = HourLinkMediaAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourLinkMediaImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = splits(3)
        val wsr_query_ald_media_id = splits(4)
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 媒体分析 渠道+小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                    kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourMediaSessionStat.stat(logRecordRdd, HourMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourMediaSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourMediaUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourMediaUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = HourMediaAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourMediaImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = splits(3)
        val ag_ald_position_id = "all"

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 位置分析 场景值+天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                        kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                        grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailyPositionSessionStat.stat(logRecordRdd, DailyPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailyPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权 PositionDailyAuthStat
    val authUserRDD = DailyPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = splits(2)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })

  }

  /**
    * 位置分析 活动+场景值+天
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyLinkPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                            kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                            grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = DailyLinkPositionSessionStat.stat(logRecordRdd, DailyLinkPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = DailyLinkPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyLinkPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = DailyLinkPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyLinkPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权 PositionDailyAuthStat
    val authUserRDD = DailyLinkPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, DailyLinkPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = "all"
        val wsr_query_ald_link_key = splits(2)
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = splits(3)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })

  }

  /**
    * 位置分析 活动+场景值+小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourLinkPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                           kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                           grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourLinkPositionSessionStat.stat(logRecordRdd, HourLinkPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourLinkPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourLinkPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourLinkPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权 PositionDailyAuthStat
    val authUserRDD = HourLinkPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourLinkPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = splits(3)
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = splits(4)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })

  }

  /**
    * 位置分析 场景值+小时
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                       kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                       grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = HourPositionSessionStat.stat(logRecordRdd, HourPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourPositionSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = HourPositionUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权 PositionDailyAuthStat
    val authUserRDD = HourPositionAuthStat.newAuthUserWithUV(baseRedisKey, dateStr, HourPositionImgSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val wsr_query_ald_link_key = "all"
        val wsr_query_ald_media_id = "all"
        val ag_ald_position_id = splits(3)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1

        val new_auth_user = row._2._2._2

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

        val sqlInsertOrUpdate =returnSql(app_key,day,hour,wsr_query_ald_link_key,wsr_query_ald_media_id,
          ag_ald_position_id,new_auth_user,visitor_count,open_count,total_page_count,
          new_comer_count,total_stay_time,secondary_avg_stay_time,one_page_count,
          bounce_rate)

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        } else {
          if (isGrey(grey_map, app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          } else {
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }
  def returnSql (app_key:String,day:String,hour:String,wsr_query_ald_link_key:String,wsr_query_ald_media_id:String,
                 ag_ald_position_id:String,new_auth_user:Long,visitor_count:Long,open_count:Long,total_page_count:Long,
                 new_comer_count:Long,total_stay_time:Long,secondary_avg_stay_time:Float,one_page_count:Long,
                 bounce_rate:Float):String = {
    val sql = s"""
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
