package com.ald.stat.appletJob.offline.independentLink.task

import com.ald.stat.appletJob.task.TaskTrait
import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.IndependentLink.{DailySessionIndependentLinkSubDimensionKey, HourIndependentLinkSessionSubDimensionKey}
import com.ald.stat.component.dimension.newLink.amount.{FullAmountDailyOpenidSubDimensionKey, FullAmountHourOpenidSubDimensionKey, FullAmountOpenidImgSubDimensionKey}
import com.ald.stat.component.dimension.newLink.linkMediaPosition._
import com.ald.stat.component.dimension.trend.{DailySessionSubDimensionKey, DailyUidSubDimensionKey, HourSessionSubDimensionKey}
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.session.newLink._
import com.ald.stat.module.uv.newLink._
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 外链分析
  * 包括：外链分析 媒体分析  位置分析
  * Created by admin on 2018/5/27.
  */
object LinkIndependentTask extends TaskTrait {

  def allStat_first(dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    ssc: SparkSession, currentTime: Long, linkAuthColumn:String, day_wz: String, logRecordRdd_2: RDD[LogRecord],
                    authUserTableName: String, totalCurrentTime: Long): Unit = {

    hourLinkMediaPositionStat(dateStr, logRecordRdd, kafkaProducer, ssc, currentTime, linkAuthColumn, day_wz, logRecordRdd_2, authUserTableName, totalCurrentTime)
  }


  /**
    * 外链分析 头部汇总 天
    *
    * @param kafkaProducer
    */

  def dailySummaryStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)], logRecordRdd_2: RDD[LogRecord],
                       dateStr: String, currentTime: Long, day_wz: String, amountAuthColumn: String, authUserTableName: String,
                       amountAuthUserRDD: RDD[(String, Long)]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"

    val tempSessMergeResult = FullAmountSessionStat.stat(logRecordRdd_2, DailySessionIndependentLinkSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDDAmount = FullAmountSessionStat.doCacheLinkOffline(dateStr, tempSessMergeResult, DailySessionIndependentLinkSubDimensionKey)
    val sessionRDDAmountStr = sessionRDDAmount.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str,record._2)
    })
    val amountAuthUserRDDStr = amountAuthUserRDD.map(record => {
      val splits = record._1.split(":")
      val Str = splits(0) + ":" + splits(1) + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str,record._2)
    }).reduceByKey(_+_)
    //uv
    val uvRDDAmount = FullAmountUVStat.fullAmountStatUV(FullAmountDailyOpenidSubDimensionKey, logRecordRdd_2)
    val uvRDDAmountStr = uvRDDAmount.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      (Str,record._2)
    })
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailySummaryAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

//    println("daily authUserRDD -------------------" + authUserRDD.count())

    val tmpRDD = sessionRDDAmountStr.join(uvRDDAmountStr).join(amountAuthUserRDDStr)
//    println("daily tmpRDD -------------------" + tmpRDD.count())
    val finalRDDLink = uvRDD.join(sessionRDD).join(authUserRDD)
//    println("daily finalRDDLink -------------------" + finalRDDLink.count())
    val finalRDD = finalRDDLink.join(tmpRDD)
//    println("daily finalRDD -------------------" + finalRDD.count())
//    println("daily ---------------------" + finalRDD.count())
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
        val visitor_count = row._2._1._1._1
        val total_visitor_count = row._2._2._1._2
        val total_new_auth_user = row._2._2._2
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._1._2
        if(total_newer_for_app > total_visitor_count){
          total_newer_for_app = total_visitor_count
        }
        if(new_comer_count > visitor_count){
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
//      })
    })
  }

  def hourSummaryStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)], logRecordRdd_2: RDD[LogRecord],
                       dateStr: String, currentTime: Long, day_wz: String, linkAuthColumn: String, authUserTableName: String): Unit = {

    var hour = ""
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"

    val tempSessMergeResult = FullAmountSessionStat.stat(logRecordRdd_2, HourIndependentLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理，所有访问小程序的会话计算
    val sessionRDDAmount = FullAmountSessionStat.doCacheLinkOffline(dateStr, tempSessMergeResult, HourIndependentLinkSessionSubDimensionKey)
    val sessionRDDAmountStr = sessionRDDAmount.map(record => {
      val Str = record._1.toString +  ":" + "all" + ":" + "all" + ":" + "all"
      (Str,record._2)
    })
    println("*******sessionRDDAmountStr count is "+sessionRDDAmountStr.count()+"*******")

    //所有访问小程序的新授权用户计算
    val amountAuthUserRDD = FullAmountUVStat.newAuthUserForIndependentLink(dateStr, FullAmountOpenidImgSubDimensionKey, logRecordRdd_2, currentTime, linkAuthColumn, day_wz, authUserTableName)
    val amountAuthUserRDDStr = amountAuthUserRDD.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all"
      (Str,record._2)
    })
    println("*******amountAuthUserRDDStr count is "+amountAuthUserRDDStr.count()+"*******")

    amountAuthUserRDDStr.cache()
    //所有访问小程序的uv计算
    val uvRDDAmount = FullAmountUVStat.fullAmountStatUV(FullAmountHourOpenidSubDimensionKey, logRecordRdd_2)
    val uvRDDAmountStr = uvRDDAmount.map(record => {
      val Str = record._1.toString + ":" + "all" + ":" + "all" + ":" + "all"
      (Str,record._2)
    })
    println("**uvRDDAmountStr count is "+uvRDDAmountStr.count()+"*******")
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailySummaryAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val tmpRDD = sessionRDDAmountStr.join(uvRDDAmountStr).join(amountAuthUserRDDStr)
    val finalRDDLink = uvRDD.join(sessionRDD).join(authUserRDD)
    val finalRDD = finalRDDLink.join(tmpRDD)
    println("***********finalRDD.Count():"+finalRDD.count()+"*************")
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
      val visitor_count = row._2._1._1._1
      val total_visitor_count = row._2._2._1._2
      val total_new_auth_user = row._2._2._2
      val open_count = sessionSum.sessionCount
      val new_auth_user = row._2._1._2
      if(total_newer_for_app > total_visitor_count){
        total_newer_for_app = total_visitor_count
      }
      if(new_comer_count > visitor_count){
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
      //      })
    })
    dailySummaryStat(session, kafkaProducer, uv, authUser, logRecordRdd_2, dateStr, currentTime, day_wz ,linkAuthColumn, authUserTableName, amountAuthUserRDDStr)
  }
  /**
    * 外链分析 活动+天
    *
    * @param kafkaProducer
    */
  def dailyLinkStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)]): Unit = {

    var hour = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_link_key = ""

//    val sessionRDD = DailyLinkSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailyLinkAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

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
        var new_comer_count = 0l
        new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if(new_comer_count > visitor_count){
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
    * 外链分析 活动 + 渠道 + 场景值+小时
    *
    * @param dateStr
    * @param logRecordRdd
    */
  def hourLinkMediaPositionStat(dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
                                ssc: SparkSession, currentTime:Long, linkAuthColumn:String, day_wz: String, logRecordRdd_2: RDD[LogRecord],
                                authUserTableName: String, totalCurrentTime: Long): Unit = {

    val tempSessMergeResult = HourLinkMediaPositionSessionStat.statOfflineLink_2(logRecordRdd, HourLinkMediaPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = HourLinkMediaPositionSessionStat.doCacheLinkOffline(dateStr, tempSessMergeResult, HourLinkMediaPositionSessionSubDimensionKey)
    sessionRDD.cache()
//    println("sessionRDD----------------------" + sessionRDD.count())
    //uv
//    val uvRDD = HourLinkMediaPositionUVStat.statIncreaseCacheWithPVLinkOffline(dateStr, HourLinkMediaPositionUidSubDimensionKey, logRecordRdd)
    val uvRDD = HourLinkMediaPositionUVStat.statUVOffline_2(HourLinkMediaPositionOpenidSubDimensionKey, logRecordRdd)
    uvRDD.cache()
//    println("uvRDD----------------------" + uvRDD.count())
    //新增授权
    val authUserRDD = HourLinkMediaPositionAuthStat.newAuthUserForIndependentLink(dateStr, HourLinkMediaPositionImgOpenidSubDimensionKey, logRecordRdd, currentTime, linkAuthColumn, day_wz, authUserTableName)
    authUserRDD.cache()
//    println("authUserRDD----------------------" + authUserRDD.count())

    hourSummaryStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD, logRecordRdd_2, dateStr, totalCurrentTime, day_wz ,linkAuthColumn, authUserTableName)
    dailyLinkStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
    hourLinkStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
    dailyMediaStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
    hourMediaStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
    dailyPositionStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
    hourPositionStat(sessionRDD, kafkaProducer, uvRDD, authUserRDD)
  }
  /**
    *
    *
    * @param dateStr
    * @param logRecordRdd_2 访问小程序的全量数据
    */
  def fullAmountStat(dateStr: String, logRecordRdd_2: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]],
                                ssc: SparkSession, currentTime:Long, hbaseColumn:String, day_wz: String): Unit = {

    val tempSessMergeResult = FullAmountSessionStat.stat(logRecordRdd_2, DailySessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = FullAmountSessionStat.doCacheLinkOffline(dateStr, tempSessMergeResult, DailySessionSubDimensionKey)
    //uv
    val uvRDD = FullAmountUVStat.fullAmountStatUV(FullAmountHourOpenidSubDimensionKey, logRecordRdd_2)

    val finalRDD = uvRDD.join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_advertise_home
             |(
             |app_key,
             |day,
             |total_visitor_count,
             |total_newer_for_app,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$visitor_count",
             | "$new_comer_count",
             | now()
             | )
             | on duplicate key update
             | total_visitor_count="$visitor_count",
             | total_newer_for_app="$new_comer_count",
             | update_at = now()
          """.stripMargin

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 外链分析 活动+小时
    *
    * @param kafkaProducer
    */
  def hourLinkStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                   uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)]): Unit = {
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_link_key = ""

//    val sessionRDD = HourLinkSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = HourLinkAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)
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
        var new_comer_count = 0l
        new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if(new_comer_count > visitor_count){
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
  def dailyMediaStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                     uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var wsr_query_ald_media_id = ""

//    val sessionRDD = DailyMediaSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailyMediaAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)
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
        var new_comer_count = 0l
        new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if(new_comer_count > visitor_count){
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
  def hourMediaStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                    uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)]): Unit = {

    var wsr_query_ald_link_key = "all"
    var ag_ald_position_id = "all"
    var hour = ""
    var wsr_query_ald_media_id = ""

//    val sessionRDD = HourMediaSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = HourMediaAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

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
        var new_comer_count = 0l
        new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if(new_comer_count > visitor_count){
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
  def dailyPositionStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                        uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)]): Unit = {

    var hour = "all"
    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var ag_ald_position_id = ""

//    val sessionRDD = DailyPositionSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = DailyPositionAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

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
        var new_comer_count = 0l
        new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if(new_comer_count > visitor_count){
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
  def hourPositionStat(session: RDD[(DimensionKey, SessionSum)], kafkaProducer: Broadcast[KafkaSink[String, String]],
                       uv: RDD[(DimensionKey, Long)], authUser: RDD[(DimensionKey, Long)]): Unit = {

    var wsr_query_ald_link_key = "all"
    var wsr_query_ald_media_id = "all"
    var hour = ""
    var ag_ald_position_id = ""

//    val sessionRDD = HourPositionSessionStat.reduceSession(resultArray, ssc, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)
    val sessionRDD = session.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })

    val uvRDD = uv.map(record => {
      val Str = record._1.toString
      (Str,record._2)
    })
    val authUserRDD = HourPositionAuthStat.reduceAuthIndependentLink(authUser, hour, wsr_query_ald_link_key, wsr_query_ald_media_id, ag_ald_position_id)

    val finalRDD = uvRDD.join(sessionRDD).join(authUserRDD)

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
        var new_comer_count = 0l
        new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1
        val open_count = sessionSum.sessionCount
        val new_auth_user = row._2._2
        if(new_comer_count > visitor_count){
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
