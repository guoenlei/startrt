package com.ald.stat.appletJob.task

import java.util

import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.component.dimension.share.pageShare.{DailyPageShareSessionSubDimensionKey, DailyPageShareUidSubDimensionKey}
import com.ald.stat.component.dimension.share.share._
import com.ald.stat.component.dimension.share.userShare.{DailyUserShareSessionSubDimensionKey, DailyUserShareUidSubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.share.{PageShareDailySessionStat, ShareDailySessionStat, ShareHourlySessionStat, UserShareDailySessionStat}
import com.ald.stat.module.session.{SessionBaseImpl, SessionStatImpl}
import com.ald.stat.module.uv.share._
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{KafkaSink, RddUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.metrics
import org.apache.spark.metrics.source
import org.apache.spark.rdd.RDD

/**
 * 分享概况任务
 * Created by admin on 2018/6/4.
 */
object ShareAllTask extends TaskTrait {

  /**
   * 分享概况  每日统计
   *
   * @param baseRedisKey
   * @param taskId
   * @param dateStr
   * @param logRecord_click_rdd
   * @param logRecord_status_rdd
   * @param kafkaProducer
   * @param redisPrefix
   */
  def shareDailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecord_click_rdd: RDD[LogRecord], logRecord_status_rdd: RDD[LogRecord],
                     kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                     grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = SessionStatImpl.stat(logRecord_click_rdd, DailyShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = ShareDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyShareSessionSubDimensionKey, redisPrefix)
    //分享人数  and 分享次数
    val share_pv_uv_rdd = ShareDailyStatusUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyShareUidSubDimensionKey, logRecord_status_rdd, redisPrefix)
    //分享被打开次数(回流量)  and  分享被打开人数
    val back_uv_op_rdd = ShareDailyClickUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyShareUidSubDimensionKey, logRecord_click_rdd, redisPrefix)

    val finalRDD = RddUtils.shareRddUnionHandle(baseRedisKey, dateStr, ShareDailyStatusUVStat.name, ShareDailyClickUVStat.name, ShareDailySessionStat.name, share_pv_uv_rdd, back_uv_op_rdd, sessionRDD, redisPrefix)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(2)
        val te = splits(1)

        val sessionSum = row._2._2._2
        val share_new_count = sessionSum.newUserCount
        val share_user_count = row._2._1._2
        val share_count = row._2._1._1
        val share_open_count = sessionSum.sessionCount
        val share_open_user_count = row._2._2._1._2

        //分享回流比
        var share_reflux_ratio = 0f
        if (share_count != 0) {
          share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_share_summary
             |(
             |app_key,
             |day,
             |share_user_count,
             |new_count,
             |share_count,
             |share_open_count,
             |share_open_user_count,
             |share_reflux_ratio,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$share_user_count",
             |"$share_new_count",
             |"$share_count",
             |"$share_open_count",
             |"$share_open_user_count",
             |"$share_reflux_ratio",
             |now()
             |)
             |ON DUPLICATE KEY UPDATE
             |share_user_count="$share_user_count",
             |new_count="$share_new_count",
             |share_count="$share_count",
             |share_open_count="$share_open_count",
             |share_open_user_count="$share_open_user_count",
             |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
             |update_at = now()
          """.stripMargin

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
        if (isOnLine) {
          sendToKafkaWithAK(kafka, sqlInsertOrUpdate, (app_key + ":" + te)) //如果上线状态为1，直接入生产kafka
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
   * 分享概况  分时统计
   *
   * @param baseRedisKey
   * @param taskId
   * @param dateStr
   * @param logRecord_click_rdd
   * @param logRecord_status_rdd
   * @param kafkaProducer
   * @param redisPrefix
   */
  def shareHourStat(baseRedisKey: String, taskId: String, dateStr: String, logRecord_click_rdd: RDD[LogRecord], logRecord_status_rdd: RDD[LogRecord],
                    kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                    grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecord_click_rdd, HourShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = ShareHourlySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourShareSessionSubDimensionKey, redisPrefix)

    //分享人数  and 分享次数
    val share_pv_uv_rdd = ShareHourlyStatusUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourShareUidSubDimensionKey, logRecord_status_rdd, redisPrefix)
    //分享被打开次数(回流量)  and  分享给打开人数
    val back_uv_op_rdd = ShareHourlyClickUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourShareUidSubDimensionKey, logRecord_click_rdd, redisPrefix)

    //最终结果集
    val finalRDD = RddUtils.shareRddUnionHandle(baseRedisKey, dateStr, ShareHourlyStatusUVStat.name, ShareHourlyClickUVStat.name, ShareHourlySessionStat.name, share_pv_uv_rdd, back_uv_op_rdd, sessionRDD, redisPrefix)


    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(2)
        val hour = splits(3)
        val te = splits(1)

        val sessionSum = row._2._2._2
        val share_new_count = sessionSum.newUserCount
        val share_user_count = row._2._1._2
        val share_count = row._2._1._1
        val share_open_count = sessionSum.sessionCount
        val share_open_user_count = row._2._2._1._2

        //分享回流比
        var share_reflux_ratio = 0f
        if (share_count != 0) {
          share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_share_summary
             |(
             |app_key,
             |day,
             |hour,
             |share_user_count,
             |new_count,
             |share_count,
             |share_open_count,
             |share_open_user_count,
             |share_reflux_ratio,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$share_user_count",
             |"$share_new_count",
             |"$share_count",
             |"$share_open_count",
             |"$share_open_user_count",
             |"$share_reflux_ratio",
             |now()
             |)
             |ON DUPLICATE KEY UPDATE
             |share_user_count="$share_user_count",
             |new_count="$share_new_count",
             |share_count="$share_count",
             |share_open_count="$share_open_count",
             |share_open_user_count="$share_open_user_count",
             |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
             |update_at = now()
          """.stripMargin

        //数据进入kafka
        if (isOnLine) {
          sendToKafkaWithAK(kafka, sqlInsertOrUpdate, (app_key + ":" + te)) //如果上线状态为1，直接入生产kafka
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
   * 页面分享  每日统计
   *
   * @param baseRedisKey
   * @param taskId
   * @param dateStr
   * @param logRecord_click_rdd
   * @param logRecord_status_rdd
   * @param kafkaProducer
   * @param redisPrefix
   */
  def pageShareDailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecord_click_rdd: RDD[LogRecord], logRecord_status_rdd: RDD[LogRecord],
                         kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                         grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecord_click_rdd, DailyPageShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = PageShareDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyPageShareSessionSubDimensionKey, redisPrefix)

    //分享人数  and 分享次数
    val share_pv_uv_rdd = PageShareDailyStatusUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyPageShareUidSubDimensionKey, logRecord_status_rdd, redisPrefix)
    //分享被打开次数(回流量)  and  分享给打开人数
    val back_uv_op_rdd = PageShareDailyClickUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyPageShareUidSubDimensionKey, logRecord_click_rdd, redisPrefix)

    //最终结果集
    val finalRDD = RddUtils.shareRddUnionHandle(baseRedisKey, dateStr, PageShareDailyStatusUVStat.name, PageShareDailyClickUVStat.name, PageShareDailySessionStat.name, share_pv_uv_rdd, back_uv_op_rdd, sessionRDD, redisPrefix)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(2)
        val path = splits(3)
        // source:分享源。长度大于255，切割成255（数据库表结构varchar=255）。
        val source = if (splits(4).length > 255) splits(4).substring(0, 255) else splits(4)
        val paras = splits(5)
        val te = splits(1)

        val sessionSum = row._2._2._2
        val share_new_count = sessionSum.newUserCount
        val share_user_count = row._2._1._2
        val share_count = row._2._1._1
        val share_open_count = sessionSum.sessionCount
        val share_open_user_count = row._2._2._1._2


        //分享回流比
        var share_reflux_ratio = 0f
        if (share_count != 0) {
          share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_dailyshare_page
             |(
             |app_key,
             |day,
             |source,
             |paras,
             |page_uri,
             |share_user_count,
             |new_count,
             |share_count,
             |share_open_count,
             |share_open_user_count,
             |share_reflux_ratio,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$source",
             |"$paras",
             |"$path",
             |"$share_user_count",
             |"$share_new_count",
             |"$share_count",
             |"$share_open_count",
             |"$share_open_user_count",
             |"$share_reflux_ratio",
             |now()
             |)
             |ON DUPLICATE KEY UPDATE
             |share_user_count="$share_user_count",
             |new_count="$share_new_count",
             |share_count="$share_count",
             |share_open_count="$share_open_count",
             |share_open_user_count="$share_open_user_count",
             |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
             |update_at = now()
          """.stripMargin

        //数据进入kafka
        if (isOnLine) {
          sendToKafkaWithAK(kafka, sqlInsertOrUpdate, (app_key + ":" + te)) //如果上线状态为1，直接入生产kafka
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
   * 用户分享  每日统计
   *
   * @param baseRedisKey
   * @param taskId
   * @param dateStr
   * @param records_rdd_userShare_click
   * @param records_rdd_userShare_status
   * @param kafkaProducer
   * @param redisPrefix
   */
  def userShareDailyStat(baseRedisKey: String, taskId: String, dateStr: String, records_rdd_userShare_click: RDD[LogRecord], records_rdd_userShare_status: RDD[LogRecord],
                         kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                         grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String], prefix: String): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(records_rdd_userShare_click, DailyUserShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = UserShareDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyUserShareSessionSubDimensionKey, redisPrefix)

    //分享人数  and 分享次数
    val share_pv_uv_rdd = UserShareDailyStatusUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyUserShareUidSubDimensionKey, records_rdd_userShare_status, redisPrefix)
    //分享被打开次数(回流量)  and  分享给打开人数
    val back_uv_op_rdd = UserShareDailyClickUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyUserShareUidSubDimensionKey, records_rdd_userShare_click, redisPrefix)

    //最终结果集
    val finalRDD = RddUtils.shareRddUnionHandle(baseRedisKey, dateStr, UserShareDailyStatusUVStat.name, UserShareDailyClickUVStat.name, UserShareDailySessionStat.name, share_pv_uv_rdd, back_uv_op_rdd, sessionRDD, redisPrefix)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(row => {
              //从DimensionKey中获取
              val splits = row._1.toString.split(":")
              val app_key = splits(0)
              val day = splits(2)
              val share_uuid = splits(3)
              val layer = splits(4)
              val te = splits(1)
              var city = "未知"
              var gender = "未知"
              if (resource.exists(share_uuid + ":" + app_key) == true) {
                val cityAndGender = resource.get(share_uuid + ":" + app_key).split(":")
                city = cityAndGender(0)
                gender = cityAndGender(1)
              }

              val sessionSum = row._2._2._2
              val share_new_count = sessionSum.newUserCount
              val share_count = row._2._1._1
              val share_open_count = sessionSum.sessionCount
              val share_open_user_count = row._2._2._1._2


              //分享回流比
              var share_reflux_ratio = 0f
              if (share_count != 0) {
                share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
              }

              val sqlInsertOrUpdate =
                s"""
                   |insert into aldstat_dailyshare_user
                   |(
                   |app_key,
                   |day,
                   |sharer_uuid,
                   |gender,
                   |city,
                   |hierarchy,
                   |new_count,
                   |share_count,
                   |share_open_count,
                   |share_open_user_count,
                   |share_reflux_ratio,
                   |update_at
                   |)
                   |values
                   |(
                   |"$app_key",
                   |"$day",
                   |"$share_uuid",
                   |"$gender",
                   |"$city",
                   |"$layer",
                   |"$share_new_count",
                   |"$share_count",
                   |"$share_open_count",
                   |"$share_open_user_count",
                   |"$share_reflux_ratio",
                   |now()
                   |)
                   |ON DUPLICATE KEY UPDATE
                   |city="$city",
                   |gender="$gender",
                   |new_count="$share_new_count",
                   |share_count="$share_count",
                   |share_open_count="$share_open_count",
                   |share_open_user_count="$share_open_user_count",
                   |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
                   |update_at = now()
          """.stripMargin

              //数据进入kafka
              //sendToKafka(kafka, sqlInsertOrUpdate)
              if (isOnLine) {
                sendToKafkaWithAK(kafka, sqlInsertOrUpdate, (app_key + ":" + te)) //如果上线状态为1，直接入生产kafka
              } else {
                if (isGrey(grey_map, app_key)) {
                  sendToKafka(kafka, sqlInsertOrUpdate)
                } else {
                  sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
                }
              }
            })
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
    })
  }

  //  /**
  //    * 用户分享  每日统计  增加维度
  //    *
  //    * @param baseRedisKey
  //    * @param taskId
  //    * @param dateStr
  //    * @param records_rdd_userShare_click
  //    * @param records_rdd_userShare_status
  //    * @param kafkaProducer
  //    * @param redisPrefix
  //    */
  //  def userShareDailyStatNew(baseRedisKey: String, taskId: String, dateStr: String, records_rdd_userShare_click: RDD[LogRecord], records_rdd_userShare_status: RDD[LogRecord],
  //                         kafkaProducer: Broadcast[KafkaSink[String, String]], grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
  //                         grey_map: util.HashMap[String, String], isOnLine: Boolean, redisPrefix: Broadcast[String]): Unit = {
  //    val tempSessMergeResult = SessionStatImpl.stat(records_rdd_userShare_click, DailyUserShareSessionSubDimensionKey, SessionBaseImpl)
  //    //session处理
  //    val sessionRDD = UserShareDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyUserShareSessionSubDimensionKey, redisPrefix)
  //    //分享人数  and 分享次数
  //    val share_pv_uv_rdd = UserShareDailyStatusUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyUserShareUidSubDimensionKey, records_rdd_userShare_status, redisPrefix)
  //    //分享被打开次数(回流量)  and  分享给打开人数
  //    val back_uv_op_rdd = UserShareDailyClickUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyUserShareUidBackSubDimensionKey, records_rdd_userShare_click, redisPrefix)
  //
  //    val finalRDD = back_uv_op_rdd.join(sessionRDD)
  //
  //    //分享人数的sql
  //    share_pv_uv_rdd.foreachPartition(par => {
  //      val kafka: KafkaSink[String, String] = kafkaProducer.value
  //      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
  //      par.foreach(row => {
  //        //从DimensionKey中获取
  //        val splits = row._1.toString.split(":")
  //        val app_key = splits(0)
  //        val day = splits(1)
  //        val share_uuid = splits(2)
  //        val gender = splits(3)
  //        val province = splits(4)
  //        val city = splits(5)
  //        val hierarchy = splits(6)
  //
  //        val share_count = row._2._1
  //
  //        val sqlInsertOrUpdate =
  //          s"""
  //             |insert into aldstat_dailyshare_user
  //             |(
  //             |app_key,
  //             |day,
  //             |gender,
  //             |province,
  //             |city,
  //             |hierarchy,
  //             |sharer_uuid,
  //             |share_count,
  //             |update_at
  //             |)
  //             |values
  //             |(
  //             |"$app_key",
  //             |"$day",
  //             |"$gender",
  //             |"$province",
  //             |"$city",
  //             |"$hierarchy",
  //             |"$share_uuid",
  //             |"$share_count",
  //             |now()
  //             |)
  //             |ON DUPLICATE KEY UPDATE
  //             |share_count="$share_count",
  //             |update_at = now()
  //          """.stripMargin
  //
  //        //数据进入kafka
  //        //sendToKafka(kafka, sqlInsertOrUpdate)
  //        if (isOnLine) {
  //          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
  //        } else {
  //          if (isGrey(grey_map, app_key)) {
  //            sendToKafka(kafka, sqlInsertOrUpdate)
  //          } else {
  //            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
  //          }
  //        }
  //      })
  //    })
  //
  //    //回流的sql
  //    finalRDD.foreachPartition(par =>{
  //      val kafka: KafkaSink[String, String] = kafkaProducer.value
  //      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
  //      par.foreach(row =>{
  //        //从DimensionKey中获取
  //        val splits = row._1.toString.split(":")
  //        val app_key = splits(0)
  //        val day = splits(1)
  //        val share_uuid = splits(2)
  //
  //        val sessionSum = row._2._2
  //        val share_new_count = sessionSum.newUserCount
  //        val share_open_count = sessionSum.sessionCount
  //        val share_open_user_count = row._2._1._2
  //
  //        val sqlInsertOrUpdate =
  //          s"""
  //            |UPDATE aldstat_dailyshare_user
  //            |SET new_count = ${share_new_count}, share_open_count = ${share_open_count}, share_open_user_count = ${share_open_user_count}, update_at = now()
  //            |WHERE app_key = '${app_key}' AND day = '${day}' AND sharer_uuid = '${share_uuid}'
  //          """.stripMargin
  //        //数据进入kafka
  //        //sendToKafka(kafka, sqlInsertOrUpdate)
  //        if (isOnLine) {
  //          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
  //        } else {
  //          if (isGrey(grey_map, app_key)) {
  //            sendToKafka(kafka, sqlInsertOrUpdate)
  //          } else {
  //            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
  //          }
  //        }
  //      })
  //    })
  //  }
}
