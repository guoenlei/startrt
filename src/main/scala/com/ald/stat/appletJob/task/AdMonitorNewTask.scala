package com.ald.stat.appletJob.task

import com.ald.stat.appletJob.offline.task.LinkExpansionTaskNew.{dailyLinkStat, dailyMediaStat, dailyPositionStat, dailySummaryStat, hourLinkStat, hourMediaStat, hourPositionStat, hourSummaryStat}
import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.IndependentLink.{DailySessionIndependentLinkSubDimensionKey, HourIndependentLinkSessionSubDimensionKey}
import com.ald.stat.component.dimension.newAdMonitor.amount.{FullAmountDailyOpenidImgSubDimensionKeyNewAdMonitor, FullAmountDailyOpenidSubDimensionKeyNewAdMonitor, FullAmountHourOpenidImgSubDimensionKeyNewAdMonitor, FullAmountHourOpenidSubDimensionKeyNewAdMonitor}
import com.ald.stat.component.dimension.newAdMonitor.link.{DailyLinkSessionSubDimensionKeyNewAdMonitor, DailyLinkUidSubDimensionKeyNewAdMonitor, HourLinkSessionSubDimensionKeyNewAdMonitor, HourLinkUidSubDimensionKeyNewAdMonitor}
import com.ald.stat.component.dimension.newAdMonitor.linkMediaPosition.{HourLinkMediaPositionImgOpenidSubDimensionKeyNewAdMonitor, HourLinkMediaPositionOpenidSubDimensionKeyNewAdMonitor, HourLinkMediaPositionSessionSubDimensionKeyNewAdMonitor}
import com.ald.stat.component.dimension.newAdMonitor.media.{DailyMediaSessionSubDimensionKeyNewAdMonitor, DailyMediaUidSubDimensionKeyNewAdMonitor, HourMediaSessionSubDimensionKeyNewAdMonitor, HourMediaUidSubDimensionKeyNewAdMonitor}
import com.ald.stat.component.dimension.newAdMonitor.position.{DailyPositionSessionSubDimensionKeyNewAdMonitor, DailyPositionUidSubDimensionKeyNewAdMonitor, HourPositionSessionSubDimensionKeyNewAdMonitor, HourPositionUidSubDimensionKeyNewAdMonitor}
import com.ald.stat.component.dimension.newAdMonitor.summary.{DailySessionSubDimensionKeyNewAdMonitor, DailyUidSubDimensionKeyNewAdMonitor, HourSessionSubDimensionKeyNewAdMonitor, HourUidSubDimensionKeyNewAdMonitor}
import com.ald.stat.component.dimension.newAdMonitor.totalSummary.{DailySessionIndependentLinkSubDimensionKeyNewAdMonitor, HourSessionIndependentLinkSubDimensionKeyNewAdMonitor}
import com.ald.stat.component.dimension.newLink.amount.{FullAmountDailyOpenidSubDimensionKey, FullAmountHourOpenidSubDimensionKey, FullAmountOpenidImgSubDimensionKey}
import com.ald.stat.component.dimension.newLink.link.{DailyLinkSessionSubDimensionKey, DailyLinkUidSubDimensionKey}
import com.ald.stat.component.dimension.newLink.linkMediaPosition._
import com.ald.stat.component.dimension.newLink.summary.DailySessionSubDimensionKey
import com.ald.stat.component.dimension.trend.DailySessionSubDimensionKey
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.session.newAdMonitor.{DailyLinkAuthStatNewAdMonitor, DailyLinkSessionStatNewAdMonitor, DailyLinkUVStatNewAdMonitor, DailyMediaAuthStatNewAdMonitor, DailyMediaSessionStatNewAdMonitor, DailyMediaUVStatNewAdMonitor, DailyPositionAuthStatNewAdMonitor, DailyPositionSessionStatNewAdMonitor, DailyPositionUVStatNewAdMonitor, DailySummaryAuthStatNewAdMonitor, DailySummarySessionStatNewAdMonitor, DailySummaryUVStatNewAdMonitor, HourLinkAuthStatNewAdMonitor, HourLinkMediaPositionAuthStatNewAdMonitor, HourLinkMediaPositionSessionStatNewAdMonitor, HourLinkMediaPositionUVStatNewAdMonitor, HourLinkSessionStatNewAdMonitor, HourLinkUVStatNewAdMonitor, HourMediaAuthStatNewAdMonitor, HourMediaSessionStatNewAdMonitor, HourMediaUVStatNewAdMonitor, HourPositionAuthStatNewAdMonitor, HourPositionSessionStatNewAdMonitor, HourPositionUVStatNewAdMonitor, HourSummaryAuthStatNewAdMonitor, HourSummarySessionStatNewAdMonitor, HourSummaryUVStatNewAdMonitor}
import com.ald.stat.module.session.newLink._
import com.ald.stat.module.uv.newLink._
import com.ald.stat.utils.KafkaSink
import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * 新版广告检测实时
 * 包括：外链分析 媒体分析  位置分析
 * Created by admin on 2018/5/27.
 */
object AdMonitorNewTask extends TaskTrait {

  def allstat_first(dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String], logRecordRdd_link: RDD[LogRecord], logRecordRdd_total: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], ssc:StreamingContext, currentTime: Long, newUserTableName: String, newUserHbaseColumn: String, authUserTableName: String, linkAuthColumn: String) = {
    hourLinkMediaPositionStat(dateStr, baseRedisKey, redisPrefix, logRecordRdd_link, logRecordRdd_total, kafkaProducer, ssc, currentTime, newUserTableName, newUserHbaseColumn, authUserTableName, linkAuthColumn)
  }

  /**
   * 外链分析 头部汇总 天
   *
   * @param kafkaProducer
   */

  def dailySummaryStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String],
                       logRecordRdd_total: RDD[LogRecord], currentTime: Long, linkAuthColumn: String, authUserTableName: String): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    // 全量数据
    val tempSessMergeResultTotal = FullAmountSessionStat.stat(logRecordRdd_total, DailySessionIndependentLinkSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    println("daily tempSessMergeResultTotal： -------------------" + tempSessMergeResultTotal.count())
    //session处理
    val sessionRDDAmount = FullAmountSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResultTotal, DailySessionIndependentLinkSubDimensionKeyNewAdMonitor, redisPrefix)
    val sessionRDDAmountStr = sessionRDDAmount.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str, record._2)
    })
    println("daily sessionRDDAmount -------------------" + sessionRDDAmount.count())
    sessionRDDAmount.take(2).foreach(println)

    //uv
    val uvRDDAmount = FullAmountUVStat.fullAmountStatUV(FullAmountDailyOpenidSubDimensionKeyNewAdMonitor, logRecordRdd_total)
    val uvRDDAmountStr = uvRDDAmount.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str, record._2)
    })
    println("daily uvRDDAmount -------------------" + uvRDDAmount.count())
    uvRDDAmount.take(2).foreach(println)

    //    新增授权
    val amountAuthUserRDD = FullAmountUVStat.newAuthUserForNewAdMonitor(dateStr, FullAmountDailyOpenidImgSubDimensionKeyNewAdMonitor, logRecordRdd_total, currentTime, linkAuthColumn, authUserTableName)
    val amountAuthUserRDDStr = amountAuthUserRDD.map(record => {
      val splits = record._1.toString.split(":")
      val Str = splits(0) + ":" + splits(1) + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str, record._2)
    }).reduceByKey(_ + _)
    println("daily amountAuthUserRDD -------------------" + amountAuthUserRDD.count())
    amountAuthUserRDD.take(2).foreach(println)

    val tmpRDD = sessionRDDAmountStr.join(uvRDDAmountStr).join(amountAuthUserRDDStr)
    // TODO
    println("daily tmpRDD(total) -------------------" + tmpRDD.count())
    println("---------------------tmpRDD take 2：")
    tmpRDD.take(2).foreach(println)
    // 广告部分数据
    val tempSessMergeResult = DailySummarySessionStatNewAdMonitor.stat_2(logRecordRdd_link, DailySessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    println("daily tempSessMergeResult -------------------" + tempSessMergeResult.count())
    val sessionRDD = DailySummarySessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailySessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("daily sessionRDD -------------------" + sessionRDD.count())
    val pvAndUvRDD = DailySummaryUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, DailyUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("daily pvAndUvRDD -------------------" + pvAndUvRDD.count())
    val authUserRDD = DailySummaryAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("daily authUserRDD -------------------" + authUserRDD.count())
    val finalRDDLink = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("daily finalRDDLink -------------------" + finalRDDLink.count())
    val finalRDD = finalRDDLink.join(tmpRDD)
    // TODO
    println("daily finalRDD -------------------" + finalRDD.count())
    println("---------------------finalRDD take 2：")

    finalRDD.take(2).foreach(println)

    finalRDD.foreach(row => {
      //      println("row================" + row)
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      //      par.foreach(row => {
      val splits = row._1.toString.split(":")
      val app_key = splits(0)
      val day = splits(1)

      val sessionSum = row._2._1._1._2
      val sessionSumTotal = row._2._2._1._1
      var new_comer_count = 0l
      new_comer_count = sessionSum.newUserCount
      var total_newer_for_app = 0l
      total_newer_for_app = sessionSumTotal.newUserCount
      val visitor_count = row._2._1._1._1._2
      val total_visitor_count = row._2._2._1._2
      val total_new_auth_user = row._2._2._2
      val open_count = sessionSum.sessionCount
      val new_auth_user = row._2._1._2
      if (total_newer_for_app > total_visitor_count) {
        total_newer_for_app = total_visitor_count
      }
      if (new_comer_count > visitor_count) {
        new_comer_count = visitor_count
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

      val sqlInsertOrUpdate =
        s"""
           |insert into aldstat_advertise_home
           |(
           |app_key,
           |day,
           |link_authuser_count,
           |total_new_auth_user,
           |link_visitor_count,
           |total_visitor_count,
           |link_open_count,
           |link_newer_for_app,
           |total_newer_for_app,
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
           | "$total_new_auth_user",
           | "$visitor_count",
           | "$total_visitor_count",
           | "$open_count",
           | "$new_comer_count",
           | "$total_newer_for_app",
           | "$total_stay_time",
           | "$secondary_avg_stay_time",
           | "$one_page_count",
           | "$bounce_rate",
           | now()
           | )
           | on duplicate key update
           | link_authuser_count="$new_auth_user",
           | total_new_auth_user="$total_new_auth_user",
           | link_visitor_count="$visitor_count",
           | total_visitor_count="$total_visitor_count",
           | link_open_count="$open_count",
           | link_newer_for_app="$new_comer_count",
           | total_newer_for_app="$total_newer_for_app",
           | total_stay_time="$total_stay_time",
           | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
           | one_page_count="$one_page_count",
           | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
           | update_at = now()
          """.stripMargin

      //数据进入kafka
      sendToKafka(kafka, sqlInsertOrUpdate)
    })
  }

  def hourSummaryStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                      ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String],
                      logRecordRdd_total: RDD[LogRecord], currentTime: Long, linkAuthColumn: String, authUserTableName: String): Unit = {

    var hour = ""
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"

    println("^^^^^^^^^^^^^^^^^^^^^^^^^hour^^^^^^^^^^^^^^^^^^^^^^^^^")

    // 全量数据
    val tempSessMergeResultTotal = FullAmountSessionStat.stat(logRecordRdd_total, HourSessionIndependentLinkSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    println("hour tempSessMergeResultTotal -------------------" + tempSessMergeResultTotal.count())

    val sessionRDDAmount = FullAmountSessionStat.doCacheStr(baseRedisKey, dateStr, tempSessMergeResultTotal, HourSessionIndependentLinkSubDimensionKeyNewAdMonitor, redisPrefix)
    val sessionRDDAmountStr = sessionRDDAmount.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str, record._2)
    })
    println("hour sessionRDDAmount -------------------" + sessionRDDAmount.count())
    sessionRDDAmount.take(2).foreach(println)

    //uv
    val uvRDDAmount = FullAmountUVStat.fullAmountStatUV(FullAmountHourOpenidSubDimensionKeyNewAdMonitor, logRecordRdd_total)
    val uvRDDAmountStr = uvRDDAmount.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str, record._2)
    })
    println("hour uvRDDAmount -------------------" + uvRDDAmount.count())
    uvRDDAmount.take(2).foreach(println)

    //    新增授权
    val amountAuthUserRDD = FullAmountUVStat.newAuthUserForNewAdMonitor(dateStr, FullAmountHourOpenidImgSubDimensionKeyNewAdMonitor, logRecordRdd_total, currentTime, linkAuthColumn, authUserTableName)
    val amountAuthUserRDDStr = amountAuthUserRDD.map(record => {
      val splits = record._1.toString.split(":")
      val Str = splits(0) + ":" + splits(1) + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str, record._2)
    }).reduceByKey(_ + _)
    println("hour amountAuthUserRDD -------------------" + amountAuthUserRDD.count())
    amountAuthUserRDD.take(2).foreach(println)

    val tmpRDD = sessionRDDAmountStr.join(uvRDDAmountStr).join(amountAuthUserRDDStr)
    println("hour tmpRDD(total) -------------------" + tmpRDD.count())
    println("---------------------tmpRDD take 2：")
    tmpRDD.take(2).foreach(println)

    // 广告部分数据
    val tempSessMergeResult = HourSummarySessionStatNewAdMonitor.stat_2(logRecordRdd_link, HourSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    println("hour tempSessMergeResult -------------------" + tempSessMergeResult.count())
    val sessionRDD = HourSummarySessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("hour sessionRDD -------------------" + sessionRDD.count())

    val pvAndUvRDD = HourSummaryUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, HourUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("hour pvAndUvRDD -------------------" + pvAndUvRDD.count())

    val authUserRDD = HourSummaryAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("hour authUserRDD -------------------" + authUserRDD.count())

    val finalRDDLink = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("hour finalRDDLink -------------------" + finalRDDLink.count())
    val finalRDD = finalRDDLink.join(tmpRDD)
    // TODO
    println("hour finalRDD -------------------" + finalRDD.count())
    println("---------------------finalRDD take 2：")
    finalRDD.take(2).foreach(println)

    finalRDD.foreach(row => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val splits = row._1.toString.split(":")
      val app_key = splits(0)
      val day = splits(1)
      val hour = splits(2)

      val sessionSum = row._2._1._1._2
      val sessionSumTotal = row._2._2._1._1
      var new_comer_count = 0l
      new_comer_count = sessionSum.newUserCount
      var total_newer_for_app = 0l
      total_newer_for_app = sessionSumTotal.newUserCount
      val visitor_count = row._2._1._1._1._2
      val total_visitor_count = row._2._2._1._2
      val total_new_auth_user = row._2._2._2
      val open_count = sessionSum.sessionCount
      val new_auth_user = row._2._1._2
      if (total_newer_for_app > total_visitor_count) {
        total_newer_for_app = total_visitor_count
      }
      if (new_comer_count > visitor_count) {
        new_comer_count = visitor_count
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

      val sqlInsertOrUpdate =
        s"""
           |insert into aldstat_advertise_home_hour
           |(
           |app_key,
           |day,
           |hour,
           |link_authuser_count,
           |total_new_auth_user,
           |link_visitor_count,
           |total_visitor_count,
           |link_open_count,
           |link_newer_for_app,
           |total_newer_for_app,
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
           | "$total_new_auth_user",
           | "$visitor_count",
           | "$total_visitor_count",
           | "$open_count",
           | "$new_comer_count",
           | "$total_newer_for_app",
           | "$total_stay_time",
           | "$secondary_avg_stay_time",
           | "$one_page_count",
           | "$bounce_rate",
           | now()
           | )
           | on duplicate key update
           | link_authuser_count="$new_auth_user",
           | total_new_auth_user="$total_new_auth_user",
           | link_visitor_count="$visitor_count",
           | total_visitor_count="$total_visitor_count",
           | link_open_count="$open_count",
           | link_newer_for_app="$new_comer_count",
           | total_newer_for_app="$total_newer_for_app",
           | total_stay_time="$total_stay_time",
           | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
           | one_page_count="$one_page_count",
           | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
           | update_at = now()
          """.stripMargin

      //数据进入kafka
      sendToKafka(kafka, sqlInsertOrUpdate)
    })
  }

  /**
   * 外链分析 活动+天
   *
   * @param kafkaProducer
   */
  def dailyLinkStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]) = {

    var hour = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_link_key = ""

    val tempSessMergeResult = DailyLinkSessionStatNewAdMonitor.stat_2(logRecordRdd_link, DailyLinkSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    println("^^^^^^^^^^^^^^^^^^^^^^^^^daily link^^^^^^^^^^^^^^^^^^^^^^^^^")
    println("daily link tempSessMergeResult -------------------" + tempSessMergeResult.count())
    val sessionRDD = DailyLinkSessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailyLinkSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("daily link sessionRDD -------------------" + sessionRDD.count())
    sessionRDD.take(2).foreach(println)

    val pvAndUvRDD = DailyLinkUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, DailyLinkUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("daily link pvAndUvRDD -------------------" + pvAndUvRDD.count())
    pvAndUvRDD.take(2).foreach(println)

    val authUserRDD = DailyLinkAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("daily link authUserRDD -------------------" + authUserRDD.count())
    authUserRDD.take(2).foreach(println)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("daily link finalRDD -------------------" + finalRDD.count())
    finalRDD.take(2).foreach(println)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_link_key = splits(2)

        val sessionSum = row._2._1._2
        var new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if (new_comer_count > visitor_count) {
          new_comer_count = visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_advertise_link
             |(
             |app_key,
             |day,
             |link_key,
             |link_authuser_count,
             |link_visitor_count,
             |link_open_count,
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
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin
        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }


  /**
   * * 新版广告检测实时计算
   * * 外链分析 活动 + 渠道 + 场景值+小时
   *
   * @param dateStr
   * @param baseRedisKey
   * @param redisPrefix
   * @param logRecordRdd_link
   * @param logRecordRdd_total
   * @param kafkaProducer
   * @param ssc
   * @param currentTime
   * @param newUserTableName
   * @param newUserHbaseColumn
   * @param authUserTableName
   * @param linkAuthColumn
   */
  def hourLinkMediaPositionStat(dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String], logRecordRdd_link: RDD[LogRecord], logRecordRdd_total: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], ssc: StreamingContext, currentTime: Long,
                                newUserTableName: String, newUserHbaseColumn: String, authUserTableName: String, linkAuthColumn: String) = {
    val tempSessMergeResult = HourLinkMediaPositionSessionStatNewAdMonitor.stat_2(logRecordRdd_link, HourLinkMediaPositionSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkMediaPositionSessionStatNewAdMonitor.doCacheLink(baseRedisKey, dateStr, tempSessMergeResult, HourLinkMediaPositionSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    //uv
    val uvRDD = HourLinkMediaPositionUVStatNewAdMonitor.statIncreaseCacheWithPVLink_2(baseRedisKey, dateStr, HourLinkMediaPositionOpenidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    //新增授权
    val authUserRDD = HourLinkMediaPositionAuthStatNewAdMonitor.newAuthUserWithOpenId_2(baseRedisKey, dateStr, HourLinkMediaPositionImgOpenidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix, currentTime, newUserTableName, newUserHbaseColumn, authUserTableName, linkAuthColumn)
    //最终结果集:dimensionKey,聚合pv，uv，批次pv，uv，聚合sessionSum，批次sessionSum，聚合pv，聚合authUser
    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("---------------------finalRDD：" + finalRDD.count())
    println("---------------------finalRDD take 2：")
    finalRDD.take(2).foreach(println)

    //    finalRDD.foreach(println)

    val resultArray = finalRDD.repartition(1).mapPartitions(par => {
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
        val new_comer_count = sessionSum.newUserCount // 推广新增UV、活动为小程序带来新增用户数
        val visitor_count = row._2._1._1._1._2 // 推广总uv（与redis聚合后的uv）
        val open_count = sessionSum.sessionCount // 推广点击量、广告打开次数
        val total_page_count = row._2._1._1._1._1 // 推广总pv
        val new_auth_user = row._2._2._1._2 // 推广总新增授权用户数（与redis聚合后的auth）
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
      .persist(StorageLevel.MEMORY_AND_DISK).collect()

    println()
    println()
    dailySummaryStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix, logRecordRdd_total, currentTime, linkAuthColumn, authUserTableName)
    println()
    println()
    hourSummaryStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix, logRecordRdd_total, currentTime, linkAuthColumn, authUserTableName)
    println()
    println()
    dailyLinkStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix)
    println()
    println()
    hourLinkStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix)
    println()
    println()
    dailyMediaStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix)
    println()
    println()
    hourMediaStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix)
    println()
    println()
    dailyPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix)
    println()
    println()
    hourPositionStat(resultArray, kafkaProducer, ssc, logRecordRdd_link, dateStr, baseRedisKey, redisPrefix)
    // TODO
    println("=====================next batch=====================")
    println()
  }


  //  /**
  //   * 外链分析 活动 + 渠道 + 场景值+小时
  //   *
  //   * @param dateStr
  //   * @param logRecordRdd
  //   */
  //  def hourLinkMediaPositionStat(dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
  //                                ssc: SparkSession, currentTime: Long, linkAuthColumn: String, day_wz: String, logRecordRdd_2: RDD[LogRecord],
  //                                authUserTableName: String, totalCurrentTime: Long): Unit = {
  //
  //    val tempSessMergeResult = HourLinkMediaPositionSessionStat.stat_2(logRecordRdd, HourLinkMediaPositionSessionSubDimensionKey, SessionBaseImpl)
  //    //session处理
  //    val sessionRDD = HourLinkMediaPositionSessionStat.doCacheLink(dateStr, tempSessMergeResult, HourLinkMediaPositionSessionSubDimensionKey)
  //    sessionRDD.cache()
  //    //    println("sessionRDD----------------------" + sessionRDD.count())
  //    //uv
  //    //    val uvRDD = HourLinkMediaPositionUVStat.statIncreaseCacheWithPVLinkOffline(dateStr, HourLinkMediaPositionUidSubDimensionKey, logRecordRdd)
  //    val uvRDD = HourLinkMediaPositionUVStat.statUVOffline_2(HourLinkMediaPositionOpenidSubDimensionKey, logRecordRdd)
  //    uvRDD.cache()
  //    //    println("uvRDD----------------------" + uvRDD.count())
  //    //新增授权
  //    val authUserRDD = HourLinkMediaPositionAuthStat.newAuthUserForIndependentLink(dateStr, HourLinkMediaPositionImgOpenidSubDimensionKey, logRecordRdd, currentTime, linkAuthColumn, day_wz, authUserTableName)
  //    authUserRDD.cache()
  //    //    println("authUserRDD----------------------" + authUserRDD.count())
  //
  //    hourSummaryStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD, logRecordRdd_2, dateStr, totalCurrentTime, day_wz, linkAuthColumn, authUserTableName)
  //    dailyLinkStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
  //    hourLinkStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
  //    dailyMediaStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
  //    hourMediaStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
  //    dailyPositionStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
  //    hourPositionStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
  //  }

  //  /**
  //   *
  //   *
  //   * @param dateStr
  //   * @param logRecordRdd_2 访问小程序的全量数据
  //   */
  //  def fullAmountStat(dateStr: String, logRecordRdd_2: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
  //                     ssc: SparkSession, currentTime: Long, hbaseColumn: String, day_wz: String): Unit = {
  //
  //    val tempSessMergeResult = FullAmountSessionStat.stat(logRecordRdd_2, DailySessionSubDimensionKey, SessionBaseImpl)
  //    //session处理
  //    val sessionRDD = FullAmountSessionStat.doCacheLinkOffline(dateStr, tempSessMergeResult, DailySessionSubDimensionKey)
  //    //uv
  //    val uvRDD = FullAmountUVStat.fullAmountStatUV(FullAmountHourOpenidSubDimensionKey, logRecordRdd_2)
  //
  //    val finalRDD = uvRDD.join(sessionRDD)
  //
  //    finalRDD.foreachPartition(par => {
  //      val kafka: KafkaSink[String, String] = kafkaProducer.value
  //      par.foreach(row => {
  //        val splits = row._1.toString.split(":")
  //        val app_key = splits(0)
  //        val day = splits(1)
  //
  //        val sessionSum = row._2._2
  //        val new_comer_count = sessionSum.newUserCount
  //        val visitor_count = row._2._1
  //
  //        val sqlInsertOrUpdate =
  //          s"""
  //             |insert into aldstat_advertise_home
  //             |(
  //             |app_key,
  //             |day,
  //             |total_visitor_count,
  //             |total_newer_for_app,
  //             |update_at
  //             |)
  //             | values
  //             | (
  //             | "$app_key",
  //             | "$day",
  //             | "$visitor_count",
  //             | "$new_comer_count",
  //             | now()
  //             | )
  //             | on duplicate key update
  //             | total_visitor_count="$visitor_count",
  //             | total_newer_for_app="$new_comer_count",
  //             | update_at = now()
  //          """.stripMargin
  //
  //        //数据进入kafka
  //        sendToKafka(kafka, sqlInsertOrUpdate)
  //      })
  //    })
  //  }

  /**
   * 外链分析 活动+小时
   *
   * @param kafkaProducer
   */
  def hourLinkStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                   ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]) = {

    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_link_key = ""

    val tempSessMergeResult = HourLinkSessionStatNewAdMonitor.stat_2(logRecordRdd_link, HourLinkSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    println("^^^^^^^^^^^^^^^^^^^^^^^^^hour link^^^^^^^^^^^^^^^^^^^^^^^^^")
    println("hour link tempSessMergeResult -------------------" + tempSessMergeResult.count())

    val sessionRDD = HourLinkSessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourLinkSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("hour link sessionRDD -------------------" + sessionRDD.count())
    sessionRDD.take(2).foreach(println)

    val pvAndUvRDD = HourLinkUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, HourLinkUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("hour link pvAndUvRDD -------------------" + pvAndUvRDD.count())
    pvAndUvRDD.take(2).foreach(println)

    val authUserRDD = HourLinkAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("hour link authUserRDD -------------------" + authUserRDD.count())
    authUserRDD.take(2).foreach(println)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("hour link finalRDD -------------------" + finalRDD.count())
    finalRDD.take(2).foreach(println)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        wsr_query_ald_link_key = splits(3)

        val sessionSum = row._2._1._2
        var new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if (new_comer_count > visitor_count) {
          new_comer_count = visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_advertise_link
             |(
             |app_key,
             |day,
             |hour,
             |link_key,
             |link_authuser_count,
             |link_visitor_count,
             |link_open_count,
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
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin
        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }


  /**
   * 媒体分析 渠道+天
   *
   * @param kafkaProducer
   */
  def dailyMediaStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                     ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]) = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_media_id = ""

    println("^^^^^^^^^^^^^^^^^^^^^^^^^daily media^^^^^^^^^^^^^^^^^^^^^^^^^")
    // TOOD ADD NewAdMonitor
    val tempSessMergeResult = DailyMediaSessionStatNewAdMonitor.stat_2(logRecordRdd_link, DailyMediaSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    println("daily media tempSessMergeResult -------------------" + tempSessMergeResult.count())
    val sessionRDD = DailyMediaSessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailyMediaSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("daily media sessionRDD -------------------" + sessionRDD.count())
    sessionRDD.take(2).foreach(println)

    val pvAndUvRDD = DailyMediaUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, DailyMediaUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("daily media pvAndUvRDD -------------------" + pvAndUvRDD.count())
    pvAndUvRDD.take(2).foreach(println)

    val authUserRDD = DailyMediaAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("daily media authUserRDD -------------------" + authUserRDD.count())
    authUserRDD.take(2).foreach(println)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("daily media finalRDD -------------------" + finalRDD.count())
    finalRDD.take(2).foreach(println)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        wsr_query_ald_media_id = splits(2)

        val sessionSum = row._2._1._2
        var new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if (new_comer_count > visitor_count) {
          new_comer_count = visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_advertise_media
             |(
             |app_key,
             |day,
             |media_id,
             |media_authuser_count,
             |media_visitor_count,
             |media_open_count,
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
             |media_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/media_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/media_open_count,2),0),
             |update_at = now()
          """.stripMargin

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
                    ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]) = {

    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_media_id = ""

    println("^^^^^^^^^^^^^^^^^^^^^^^^^hour media^^^^^^^^^^^^^^^^^^^^^^^^^")

    val tempSessMergeResult = HourMediaSessionStatNewAdMonitor.stat_2(logRecordRdd_link, HourMediaSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    println("hour media tempSessMergeResult -------------------" + tempSessMergeResult.count())
    val sessionRDD = HourMediaSessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourMediaSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("hour media sessionRDD -------------------" + sessionRDD.count())
    sessionRDD.take(2).foreach(println)

    val pvAndUvRDD = HourMediaUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, HourMediaUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("hour media pvAndUvRDD -------------------" + pvAndUvRDD.count())
    pvAndUvRDD.take(2).foreach(println)

    val authUserRDD = HourMediaAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("hour media authUserRDD -------------------" + authUserRDD.count())
    authUserRDD.take(2).foreach(println)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("hour media finalRDD -------------------" + finalRDD.count())
    finalRDD.take(2).foreach(println)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        wsr_query_ald_media_id = splits(3)

        val sessionSum = row._2._1._2
        var new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if (new_comer_count > visitor_count) {
          new_comer_count = visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_advertise_media
             |(
             |app_key,
             |day,
             |hour,
             |media_id,
             |media_authuser_count,
             |media_visitor_count,
             |media_open_count,
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
             |media_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/media_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/media_open_count,2),0),
             |update_at = now()
          """.stripMargin

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
   * 位置分析 场景值+天
   *
   * @param kafkaProducer
   */
  def dailyPositionStat(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], kafkaProducer: Broadcast[KafkaSink[String, String]],
                        ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]) = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = ""

    println("^^^^^^^^^^^^^^^^^^^^^^^^^daily position^^^^^^^^^^^^^^^^^^^^^^^^^")

    val tempSessMergeResult = DailyPositionSessionStatNewAdMonitor.stat_2(logRecordRdd_link, DailyPositionSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    println("daily position tempSessMergeResult -------------------" + tempSessMergeResult.count())
    val sessionRDD = DailyPositionSessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, DailyPositionSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("daily position sessionRDD -------------------" + sessionRDD.count())
    sessionRDD.take(2).foreach(println)

    val pvAndUvRDD = DailyPositionUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, DailyPositionUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("daily position pvAndUvRDD -------------------" + pvAndUvRDD.count())
    sessionRDD.take(2).foreach(println)

    val authUserRDD = DailyPositionAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("daily position authUserRDD -------------------" + authUserRDD.count())
    authUserRDD.take(2).foreach(println)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("daily position finalRDD -------------------" + finalRDD.count())
    finalRDD.take(2).foreach(println)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        ag_ald_position_id = splits(2)

        val sessionSum = row._2._1._2
        var new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if (new_comer_count > visitor_count) {
          new_comer_count = visitor_count
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


        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_advertise_position
             |(
             |app_key,
             |day,
             |position_id,
             |position_authuser_count,
             |position_visitor_count,
             |position_open_count,
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
             |position_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/position_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/position_open_count,2),0),
             |update_at=now()
          """.stripMargin

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
                       ssc: StreamingContext, logRecordRdd_link: RDD[LogRecord], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]) = {

    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var hour = ""
    var ag_ald_position_id = ""

    println("^^^^^^^^^^^^^^^^^^^^^^^^^hour position^^^^^^^^^^^^^^^^^^^^^^^^^")

    val tempSessMergeResult = HourPositionSessionStatNewAdMonitor.stat_2(logRecordRdd_link, HourPositionSessionSubDimensionKeyNewAdMonitor, SessionBaseImpl)
    //session处理
    println("hour position tempSessMergeResult -------------------" + tempSessMergeResult.count())
    val sessionRDD = HourPositionSessionStatNewAdMonitor.doCacheStr(baseRedisKey, dateStr, tempSessMergeResult, HourPositionSessionSubDimensionKeyNewAdMonitor, redisPrefix)
    println("hour position sessionRDD -------------------" + sessionRDD.count())
    sessionRDD.take(2).foreach(println)

    val pvAndUvRDD = HourPositionUVStatNewAdMonitor.statIncreaseCacheWithPVOnline_2(baseRedisKey, dateStr, HourPositionUidSubDimensionKeyNewAdMonitor, logRecordRdd_link, redisPrefix)
    println("hour position pvAndUvRDD -------------------" + pvAndUvRDD.count())
    pvAndUvRDD.take(2).foreach(println)

    val authUserRDD = HourPositionAuthStatNewAdMonitor.reduceAuthOnline_2(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id, redisPrefix, dateStr, baseRedisKey)
    println("hour position authUserRDD -------------------" + authUserRDD.count())
    authUserRDD.take(2).foreach(println)

    val finalRDD = pvAndUvRDD.join(sessionRDD).join(authUserRDD)
    // TODO
    println("hour position finalRDD -------------------" + finalRDD.count())
    println("---------------------finalRDD take 2：")
    finalRDD.take(2).foreach(println)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        ag_ald_position_id = splits(3)

        val sessionSum = row._2._1._2
        var new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if (new_comer_count > visitor_count) {
          new_comer_count = visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_advertise_position
             |(
             |app_key,
             |day,
             |hour,
             |position_id,
             |position_authuser_count,
             |position_visitor_count,
             |position_open_count,
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
             |position_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/position_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/position_open_count,2),0),
             |update_at=now()
          """.stripMargin


        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }
}

