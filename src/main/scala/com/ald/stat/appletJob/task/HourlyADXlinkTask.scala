package com.ald.stat.appletJob.task

import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HbaseUtiles, KafkaSink}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.exceptions.JedisConnectionException

object HourlyADXlinkTask extends TaskTrait {

  /**
    * 判断是否为新用户和新增授权用户
    *
    * @param logRecordRdd
    */
  def adxLinkStat(logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], prefix: String): Unit = {
    logRecordRdd.foreachPartition(par => {
      val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
      val resource = redisCache.getResource
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      try {
        par.foreach(r => {
          val day = r.day
          val ak = r.ak
          val line_name = r.wsr_query_ald_link_name
          val media_name = r.wsr_query_ald_media_id
          val openid = r.op
          val hour = r.hour
          var is_new_user = 1
          var first_visit_time = "0000-00-00 00:00:00"
          var is_new_authorization = 1
          var authorization_time = "0000-00-00 00:00:00"
          val redisKey = r.ak + ":" + r.day + ":" + r.hour + ":" + r.op + ":" + r.wsr_query_ald_link_name + ":" + r.wsr_query_ald_media_id
          if (resource.exists(redisKey) == false) {
            val rowKey = openid + ":" + ak
            val hbaseColumn = "adxlink"
            val currentTime = r.et.toLong
            val tran_time = ComputeTimeUtils.tranTimeToString(currentTime)
            if (r.ifo == "true" && r.img != "useless_img") {
              val result = HbaseUtiles.getDataFromHTable(rowKey, currentTime, hbaseColumn)
              if (result._1 == "notStored") {
                is_new_user = 0
                is_new_authorization = 0
                first_visit_time = tran_time
                authorization_time = tran_time
              } else if (StringUtils.isNotBlank(result._1)) {
                first_visit_time = ComputeTimeUtils.tranTimeToString(result._1.toLong)
              } else if (StringUtils.isBlank(result._1)) {
                val result2 = HbaseUtiles.getDataFromHTable2(rowKey, currentTime, "offline")
                if (StringUtils.isNotBlank(result2._1) && result2._1 != "notStored") {
                  first_visit_time = ComputeTimeUtils.tranTimeToString(result2._1.toLong)
                }
              } else {
                is_new_user = 0
                is_new_authorization = 0
                first_visit_time = tran_time
                authorization_time = tran_time
                HbaseUtiles.insertTable(rowKey, hbaseColumn, currentTime)
              }
            } else if (r.ifo == "true" && r.img == "useless_img") {
              val result = HbaseUtiles.getDataFromHTable(rowKey, currentTime, hbaseColumn)
              if (result._1 == "notStored") {
                is_new_user = 0
                first_visit_time = tran_time
              } else if (StringUtils.isNotBlank(result._1)) {
                first_visit_time = ComputeTimeUtils.tranTimeToString(result._1.toLong)
              } else if (StringUtils.isBlank(result._1)) {
                val result2 = HbaseUtiles.getDataFromHTable2(rowKey, currentTime, "offline")
                if (StringUtils.isNotBlank(result2._1) && result2._1 != "notStored") {
                  first_visit_time = ComputeTimeUtils.tranTimeToString(result2._1.toLong)
                }
              } else {
                is_new_user = 0
                first_visit_time = tran_time
                HbaseUtiles.insertTable(rowKey, hbaseColumn, currentTime)
              }
            } else if (r.ifo == "false") {
              val result = HbaseUtiles.getDataFromHTable(rowKey, currentTime, hbaseColumn)
              if (result._1 != "notStored" && StringUtils.isNotBlank(result._1)) {
                first_visit_time = ComputeTimeUtils.tranTimeToString(result._1.toLong)
              }
            }
            resource.set(redisKey, "1") //不存在，则添加，并赋值为1
            resource.expireAt(redisKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
            val sqlInsertOrUpdate =
              s"""
                 |insert into ald_ad_activity_hourly
                 |(
                 |    app_key,
                 |    day,
                 |    hour,
                 |    activity,
                 |    channel,
                 |    openid,
                 |    is_new_user,
                 |    first_visit_time,
                 |    is_new_authorization,
                 |    authorization_time
                 |)
                 |values(
                 |    "$ak",
                 |    "$day",
                 |    "$hour",
                 |    "$line_name",
                 |    "$media_name",
                 |    "$openid",
                 |    "$is_new_user",
                 |    "$first_visit_time",
                 |    "$is_new_authorization",
                 |    "$authorization_time"
                 |)
                """.stripMargin
            sendToKafka(kafka, sqlInsertOrUpdate)
          }
        })
      } catch {
        case jce: JedisConnectionException => jce.printStackTrace()
      } finally {
        if (resource != null) resource.close()
        if (redisCache != null) redisCache.close()
      }
    })
  }
}
