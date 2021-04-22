package com.ald.stat.appletJob

import java.util.{Date, Properties}

import com.ald.stat.appletJob.task._
import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object AnalysisShare extends AbstractBaseJob {

  val prefix = "jobShare"
  val offsetPrefix = "share_offset"
  val baseRedisKey = "rt";

  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    var partitionNumber = 20
    if (args.length >= 1) {
      if (args(0).toInt > 0) {
        partitionNumber = args(0).toInt
      }
    }

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //      .set("spark.executor.cores", "10")
//            .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))

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

    val grey_kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("grey_kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


    //广播
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
//    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, offsetPrefix)

    /**维护最小和最大的offset*/
    stream.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(par=>{
        val dateStr = ComputeTimeUtils.getDayAndHourStr(new Date())
        val offsetRedisCache = CacheRedisFactory.getInstances(offsetPrefix).asInstanceOf[AbstractRedisCache]
        val resource_offset = offsetRedisCache.getResource
        try{
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)

          RddUtils.checkAndSaveMinOffsetNew(dateStr, o.topic, group,o.fromOffset,o.partition,resource_offset)
          RddUtils.checkAndSaveLatestOffsetNew(dateStr, o.topic, group,o.untilOffset,o.partition,resource_offset)

        }finally {
          if (resource_offset != null) resource_offset.close()
          if (offsetRedisCache != null) offsetRedisCache.close()
        }
      })
    })

    /**分区并标记新用户*/
//    val stream_rdd = stream.filter(r =>abc(r)).repartition(partitionNumber).cache()
    val stream_rdd = stream.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
       par.foreach(record =>{
         val logRecord = LogRecord.line2Bean(record.value())
         if(logRecord != null && StringUtils.isNotBlank(logRecord.tp)){
           if(logRecord.tp == "ald_share_status" || logRecord.tp == "ald_share_click"){
             recordsRdd += logRecord
           }
         }
       })
       recordsRdd.iterator
    }).repartition(partitionNumber).cache()
    stream_rdd.foreachRDD(rdd=>{
      handleNewUserForOldSDKShare(rdd, baseRedisKey, prefix)
    })

    stream_rdd.foreachRDD(rdd => {
        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())

        //分享概况和页面分享的数据
        val logRecord_click_rdd = rdd.mapPartitions(par => {
            val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
            val resource = redisCache.getResource
            val recordsRdd = ArrayBuffer[LogRecord]()
            try{
              par.foreach(logRecord => {
//                val logRecord = LogRecord.line2Bean(line.value())
                if (logRecord != null &&
                  logRecord.ev != "app" &&
                  StringUtils.isNotBlank(logRecord.ak) &&
                  StringUtils.isNotBlank(logRecord.at) &&
                  StringUtils.isNotBlank(logRecord.ev) &&
                  StringUtils.isNotBlank(logRecord.uu) &&
                  StringUtils.isNotBlank(logRecord.et) &&
                  StringUtils.isNotBlank(logRecord.tp) &&
                  StringUtils.isNotBlank(logRecord.te) &&
                  StringUtils.isNotBlank(logRecord.path) &&
                  logRecord.tp == "ald_share_click"
                ) {
                  logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                  //回流的时候是wsr_query_utm_s，主动分享的时候是ct_utm_s，为了统一字段名，这里做处理
                  logRecord.ct_utm_s = logRecord.wsr_query_utm_s
                  logRecord.ct_utm_p = logRecord.wsr_query_utm_p
                  //将path赋给pp,防止session计算报错
                  if(logRecord.path.size>1 && logRecord.path.substring(0,1) == "/"){
                    logRecord.path = logRecord.path.substring(1)
                  }
                  logRecord.pp = logRecord.path
                  //把新用户的会话中的所有记录都标记为ifo=true
                  markNewUser(logRecord, resource, baseRedisKey)
                  //只需要 ev = event
                  if (logRecord.scene == "1007" || logRecord.scene == "1008" || logRecord.scene == "1044" || logRecord.scene == "1036" || logRecord.scene == "1074"){
                  if (logRecord.ev == "event") {
                    if(logRecord.v >= "7.0.0"){
                        recordsRdd += logRecord
                    }else{
                      recordsRdd += logRecord
                    }
                  }
                  }
                }
              })
            }catch {
              case jce: JedisConnectionException => jce.printStackTrace()
            } finally {
              if (resource != null) resource.close()
              if (redisCache != null) redisCache.close()
            }

            recordsRdd.iterator
          }).cache()

        val logRecord_status_rdd = rdd.mapPartitions(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            use(redisCache.getResource) {
              resource =>
                par.foreach(logRecord => {
//                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    logRecord.ev != "app" &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.te) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_status"
                  ) {
                    logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                    if(logRecord.path.size>1 && logRecord.path.substring(0,1) == "/"){
                      logRecord.path = logRecord.path.substring(1)
                    }
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path
                    //把新用户的会话中的所有记录都标记为ifo=true
                    markNewUser(logRecord, resource, baseRedisKey)
                    //只需要 ev = event
                    if (logRecord.ev == "event") {
                      if(logRecord.v >= "7.0.0"){
                          recordsRdd += logRecord
                      }else{
                        recordsRdd += logRecord
                      }
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        //用户分享所需数据
        val records_rdd_userShare_click = rdd.mapPartitions(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            use(redisCache.getResource) {
              resource =>
                par.foreach(logRecord => {
//                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    logRecord.ev != "app" &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.te) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_click"
                  ) {
                    logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                    if(logRecord.path.size>1 && logRecord.path.substring(0,1) == "/"){
                      logRecord.path = logRecord.path.substring(1)
                    }
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path
                    //把新用户的会话中的所有记录都标记为ifo=true
                    markNewUser(logRecord, resource, baseRedisKey)
                    //从wsr_query_ald_share_src中获取分享源，取最后一个作为分享用户
                    if (StringUtils.isNotBlank(logRecord.wsr_query_ald_share_src)) {
                      val src_arr = logRecord.wsr_query_ald_share_src.split(",")
                      if (src_arr.length >= 1) {
                        logRecord.src = src_arr(src_arr.length - 1) //取最后一个uu，做为分享源
                        logRecord.layer = src_arr.length.toString  //判断是几度分享
                      }
                    }

                    //只需要ev = event 记录
                    if (logRecord.scene == "1007" || logRecord.scene == "1008" || logRecord.scene == "1044" || logRecord.scene == "1036" || logRecord.scene == "1074") {
                      if (logRecord.ev == "event" && StringUtils.isNotBlank(logRecord.src)) {
                        if (logRecord.v >= "7.0.0") {
                          recordsRdd += logRecord
                        } else {
                          recordsRdd += logRecord
                        }
                      }
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val records_rdd_userShare_status = rdd.mapPartitions(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            use(redisCache.getResource) {
              resource =>
                par.foreach(logRecord => {
//                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    logRecord.ev != "app" &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.te) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_status"
                  ) {
                    logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                    if(logRecord.path.size>1 && logRecord.path.substring(0,1) == "/"){
                      logRecord.path = logRecord.path.substring(1)
                    }
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path
                    //把新用户的会话中的所有记录都标记为ifo=true
                    markNewUser(logRecord, resource, baseRedisKey)
                    //从wsr_query_ald_share_src中获取分享源，取最后一个作为分享用户
                    if (StringUtils.isNotBlank(logRecord.wsr_query_ald_share_src)) {
                      val src_arr = logRecord.wsr_query_ald_share_src.split(",")
                      if (src_arr.length >= 1) {
                        logRecord.src = src_arr(src_arr.length - 1) //取最后一个uu，做为分享源
                        logRecord.layer = src_arr.length.toString  //判断是几度分享
                      }
                    }
                    //logRecord.src = logRecord.uu
                    //只需要ev = event 记录
                    if (logRecord.ev == "event" && StringUtils.isNotBlank(logRecord.src)) {
                      if(logRecord.v >= "7.0.0"){
                          recordsRdd += logRecord
                      }else{
                        recordsRdd += logRecord
                      }
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val grap_map = cacheGrapUser("online_status_share") //缓存灰度用户上线状态
        val isOnLine = isOnline("online_status_share") //上线开关

        //分享概况
        ShareAllTask.shareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        ShareAllTask.shareHourStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        //页面分享
        ShareAllTask.pageShareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
//        用户分享
        ShareAllTask.userShareDailyStat(baseRedisKey, taskId, dateStr, records_rdd_userShare_click, records_rdd_userShare_status, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, prefix)

      records_rdd_userShare_status.unpersist(false)
      records_rdd_userShare_click.unpersist(false)
      logRecord_click_rdd.unpersist(false)
      logRecord_status_rdd.unpersist(false)
      rdd.unpersist(false)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
