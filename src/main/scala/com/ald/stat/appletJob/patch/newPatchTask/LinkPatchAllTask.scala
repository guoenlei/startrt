package com.ald.stat.appletJob.patch.newPatchTask

import java.util

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.appletJob.task.TaskTrait
import com.ald.stat.component.dimension.link.link._
import com.ald.stat.component.dimension.link.media._
import com.ald.stat.component.dimension.link.position._
import com.ald.stat.component.dimension.link.summary.{LinkDimensionKey, LinkImgSubDimensionKey, LinkSessionSubDimensionKey, LinkUidSubDimensionKey}
import com.ald.stat.appletJob.task.LinkAllTask.{isGrey, sendToGreyKafka, sendToKafka}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.link._
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.uv.link._
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object LinkPatchAllTask extends TaskTrait{

  def allStat_first(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                    kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    summaryStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grey_map, isOnLine, redisPrefix,patchHour,prefix)

    dailyLinkStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grey_map, isOnLine, redisPrefix,patchHour,prefix)

    hourLinkStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grey_map, isOnLine, redisPrefix,patchHour,prefix)
  }

  def allStat_second(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                     kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                     grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    dailyMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grey_map, isOnLine, redisPrefix,patchHour,prefix)

    hourMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grey_map, isOnLine, redisPrefix,patchHour,prefix)

    dailyPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grey_map, isOnLine, redisPrefix,patchHour,prefix)

    hourPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grey_map, isOnLine, redisPrefix,patchHour,prefix)
  }

  /**
    * 外链分析 头部汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def summaryStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                  kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                  grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    val tempSessMergeResult = LinkSummarySessionStat.stat(logRecordRdd, LinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkSummarySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, LinkSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = LinkDailyUVStat.statPatchSubPV(baseRedisKey, dateStr, LinkDimensionKey, logRecordRdd, redisPrefix,patchHour,prefix)
    //新增授权
    val authUserRDD = LinkSummaryAuthStat.statPatchAuth(baseRedisKey, dateStr,LinkImgSubDimensionKey,logRecordRdd, redisPrefix)

    val uvRDD = LinkSummaryUVStat.statPatchUVNew(baseRedisKey, dateStr, LinkUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1
        val new_auth_user = row._2._2

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
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 外链分析 每日汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyLinkStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                    kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    val tempSessMergeResult = LinkDailySessionStat.stat(logRecordRdd, DailyLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyLinkSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = LinkDailyUVStat.statPatchSubPV(baseRedisKey, dateStr, DailyLinkDimensionKey, logRecordRdd, redisPrefix,patchHour,prefix)
    //新增授权
    val authUserRDD = LinkDailyAuthStat.statPatchAuth(baseRedisKey, dateStr, DailyLinkImgSubDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = LinkDailyUVStat.statPatchUVNew(baseRedisKey, dateStr, DailyLinkUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val link_id = splits(2)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1
        val new_auth_user = row._2._2

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
             | "$link_id",
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
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 外链分析 每日详情
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourLinkStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                   kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                   grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    val tempSessMergeResult = LinkHourlySessionStat.stat(logRecordRdd, HourLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkHourlySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourLinkSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = LinkHourlyUVStat.statPatchSubPV(baseRedisKey, dateStr, HourLinkDimensionKey, logRecordRdd, redisPrefix,patchHour,prefix)
    //授权用户  LinkHourlyAuthStat
    val authUserRDD = LinkHourlyAuthStat.statPatchAuth(baseRedisKey, dateStr, HourLinkImgSubDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = LinkHourlyUVStat.statPatchUVNew(baseRedisKey, dateStr, HourLinkUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val link_id = splits(3)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1
        val new_auth_user = row._2._2

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
             | "$link_id",
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
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 媒体分析 每日汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                     kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                     grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    val tempSessMergeResult = MediaDailySessionStat.stat(logRecordRdd, DailyMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = MediaDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = MediaDailyUVStat.statPatchSubPV(baseRedisKey, dateStr, DailyMediaDimensionKey, logRecordRdd, redisPrefix,patchHour,prefix)
    //新增授权
    val authUserRDD = MediaDailyAuthStat.statPatchAuth(baseRedisKey, dateStr, DailyMediaImgSubDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = MediaDailyUVStat.statPatchUVNew(baseRedisKey, dateStr, DailyMediaUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val media_id = splits(2)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1
        val new_auth_user = row._2._2

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
             |"$media_id",
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
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  /**
    * 媒体分析 每日详情
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                    kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    val tempSessMergeResult = MediaHourSessionStat.stat(logRecordRdd, HourMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = MediaHourSessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = MediaHourlyUVStat.statPatchSubPV(baseRedisKey, dateStr, HourMediaDimensionKey, logRecordRdd, redisPrefix,patchHour,prefix)
    //新增授权
    val authUserRDD = MediaHourlyAuthStat.statPatchAuth(baseRedisKey, dateStr, HourMediaImgSubDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = MediaHourlyUVStat.statPatchUVNew(baseRedisKey, dateStr, HourMediaUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val media_id = splits(3)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1
        val new_auth_user = row._2._2

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
             |"$media_id",
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
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })

  }

  /**
    * 位置分析 每日汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                        kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                        grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {

    val tempSessMergeResult = PositionDailySessionStat.stat(logRecordRdd, DailyPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = PositionDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = PositionDailyUVStat.statPatchSubPV(baseRedisKey, dateStr, DailyPositionDimensionKey, logRecordRdd, redisPrefix,patchHour,prefix)

    val uvRDD = PositionDailyUVStat.statPatchUVNew(baseRedisKey, dateStr, DailyPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权 PositionDailyAuthStat
    val authUserRDD = PositionDailyAuthStat.statPatchAuth(baseRedisKey, dateStr, DailyPositionImgSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val position_id = splits(2)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1
        val new_auth_user = row._2._2

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
             |"$position_id",
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
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })

  }

  /**
    * 位置分析 每日详情
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                       kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                       grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String],patchHour:String,prefix:String): Unit = {
    val tempSessMergeResult = PositionHourlySessionStat.stat(logRecordRdd, HourPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = PositionHourlySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = PositionHourlyUVStat.statPatchSubPV(baseRedisKey, dateStr, HourPositionDimensionKey, logRecordRdd, redisPrefix,patchHour,prefix)

    val uvRDD = PositionHourlyUVStat.statPatchUVNew(baseRedisKey, dateStr, HourPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //新增授权
    val authUserRDD = PositionHourlyAuthStat.statPatchAuth(baseRedisKey, dateStr, HourPositionImgSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD).join(authUserRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val position_id = splits(3)

        val sessionSum = row._2._1._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1._1
        val new_auth_user = row._2._2

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
             |"$position_id",
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
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

}
