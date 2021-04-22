package com.ald.stat.appletJob.patch.newPatchJob

import java.util.{Date, Properties}

import com.ald.stat.appletJob.AbstractBaseJob
import com.ald.stat.appletJob.patch.newPatchTask.SharePatchAllTask
import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object AnalysisSharePatch extends AbstractBaseJob {

  val prefix = "jobShare"
  val offsetPrefix = "share_offset"
  val baseRedisKey = "rt";

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    var partitionNumber = 20
    var dateStr = ComputeTimeUtils.getDateStr(new Date())
    var patchHour = ""
    var patchType = GlobalConstants.PATCH_TYPE_CACHE_LATEST
    if (args.length >= 1) {
      if (args(0).toInt > 0) {
        try {
          partitionNumber = args(0).toInt
        } catch {
          case e: NumberFormatException => {
            println(s"$args(0)")
            e.printStackTrace()
          }
        }
      }
      if (args.length >= 2) {
        if (args(1).toInt > 9) {
          patchHour = args(1)
        } else {
          patchHour = "0" + args(1)
        }
      }
      //判断补偿逻辑
      if (args.length >= 3) {
        try {
          patchType = args(2)
        } catch {
          case e: NumberFormatException => {
            println(s"$args(2)")
            e.printStackTrace()
          }
        }
      }
    }

    println(s"$partitionNumber,$dateStr,$patchType")

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //                  .set("spark.executor.cores", "2")
      //      .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
    //rdd patch
    rddPatch(sparkConf, taskId, dateStr, partitionNumber, patchType, patchHour)
  }


  /**
    *
    * @param sparkConf
    * @param partitionNumber
    */
  def rddPatch(sparkConf: SparkConf, taskId: String, dateStr: String, partitionNumber: Int, patchType: String, patchHour: String): Unit = {

    val sparkContext = new SparkContext(sparkConf)
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val grey_kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("grey_kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val redisPrefix = sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")

    val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
    use(redisCache.getResource) {
      resource =>
        val listBuffer = RedisUtils.getClusterKeys(prefix)
        for (key <- listBuffer) {
          var markedKey = key + ":" + prefix //将集群redis中的key都拿出来并做标记之后备份
          resource.set(markedKey, resource.get(key))
          resource.expireAt(markedKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
    }
    var loop = 0
    breakable {
      while (true) {
        loop = loop + 1
        println("loop:" + loop)
        val rddAndExit = KafkaConsume.rddFromOffsetRangeToPatchBatch(sparkContext, topic, group, baseRedisKey, offsetPrefix, patchHour)
        val re_rdd = rddAndExit._1.repartition(partitionNumber)

        //标记新用户
        handleNewUserForOldSDK(re_rdd, baseRedisKey, prefix)

        //存储用户的性别和城市
        re_rdd.foreachPartition(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val resource = redisCache.getResource
          try {
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (null != logRecord && StringUtils.isNotBlank(logRecord.uu) && StringUtils.isNotBlank(logRecord.ak)) {
                if (StringUtils.isBlank(logRecord.gender)) {
                  logRecord.gender = "未知"
                } else if (logRecord.gender == "1") {
                  logRecord.gender = "男"
                } else if (logRecord.gender == "2") {
                  logRecord.gender = "女"
                } else {
                  logRecord.gender = "未知"
                }
                val srcKey = logRecord.uu + ":" + logRecord.ak
                if (resource.exists(srcKey)) {
                  if (StringUtils.isNotBlank(logRecord.city) && logRecord.gender != "未知") {
                    resource.set(srcKey, (logRecord.city + ":" + logRecord.gender))
                    resource.expireAt(srcKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  }
                } else {
                  if (StringUtils.isNotBlank(logRecord.city)) {
                    resource.set(srcKey, (logRecord.city + ":" + logRecord.gender))
                    resource.expireAt(srcKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  }
                }
              }
            })
          } catch {
            case jce: JedisConnectionException => jce.printStackTrace()
          } finally {
            if (resource != null) resource.close()
            if (redisCache != null) redisCache.close()
          }
        })

        val logRecord_click_rdd: RDD[LogRecord] = re_rdd.mapPartitions(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val resource = redisCache.getResource

          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try {
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                logRecord.ev != "app" &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.tp) &&
                StringUtils.isNotBlank(logRecord.path) &&
                logRecord.tp == "ald_share_click"
              ) {
                logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
                //将path赋给pp,防止session计算报错
                if (logRecord.path.size > 1 && logRecord.path.substring(0, 1) == "/") {
                  logRecord.path = logRecord.path.substring(1)
                }
                logRecord.pp = logRecord.path
                //把新用户的会话中的所有记录都标记为ifo=true
                markNewUser(logRecord, resource, baseRedisKey)
                //只需要 ev = event
                if (logRecord.ev == "event") {
                  recordsRdd += logRecord
                }
              }
            })
          } catch {
            case jce: JedisConnectionException => jce.printStackTrace()
          } finally {
            if (resource != null) resource.close()
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val logRecord_status_rdd: RDD[LogRecord] = re_rdd.repartition(partitionNumber).mapPartitions(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val resource = redisCache.getResource
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try {
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                logRecord.ev != "app" &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.tp) &&
                StringUtils.isNotBlank(logRecord.path) &&
                logRecord.tp == "ald_share_status"
              ) {
                logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
                if (logRecord.path.size > 1 && logRecord.path.substring(0, 1) == "/") {
                  logRecord.path = logRecord.path.substring(1)
                }
                //将path赋给pp,防止session计算报错
                logRecord.pp = logRecord.path
                //把新用户的会话中的所有记录都标记为ifo=true
                markNewUser(logRecord, resource, baseRedisKey)
                //只需要 ev = event
                if (logRecord.ev == "event") {
                  recordsRdd += logRecord
                }
              }
            })
          } finally {
            if (resource != null) resource.close()
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val records_rdd_userShare_click: RDD[LogRecord] = re_rdd.repartition(partitionNumber).mapPartitions(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val resource = redisCache.getResource
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try {
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                logRecord.ev != "app" &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.tp) &&
                StringUtils.isNotBlank(logRecord.path) &&
                logRecord.tp == "ald_share_click"
              ) {
                logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
                if (logRecord.path.size > 1 && logRecord.path.substring(0, 1) == "/") {
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
                    logRecord.layer = src_arr.length.toString //判断是几度分享
                  }
                }

                //只需要ev = event 记录
                if (logRecord.ev == "event" && StringUtils.isNotBlank(logRecord.src)) {
                  recordsRdd += logRecord
                }
              }
            })
          } finally {
            if (resource != null) resource.close()
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val records_rdd_userShare_status = re_rdd.repartition(partitionNumber).mapPartitions(par => {
          val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
          val resource = redisCache.getResource
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try {
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                logRecord.ev != "app" &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.tp) &&
                StringUtils.isNotBlank(logRecord.path) &&
                logRecord.tp == "ald_share_status"
              ) {
                logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
                if (logRecord.path.size > 1 && logRecord.path.substring(0, 1) == "/") {
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
                    logRecord.layer = src_arr.length.toString //判断是几度分享
                  }
                }
                //只需要ev = event 记录
                if (logRecord.ev == "event" && StringUtils.isNotBlank(logRecord.src)) {
                  recordsRdd += logRecord
                }
              }
            })
          } finally {
            if (resource != null) resource.close()
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()
        val grap_map = cacheGrapUser("online_status_share") //缓存灰度用户上线状态
        val isOnLine = isOnline("online_status_share") //上线开关

        //分享概况
        SharePatchAllTask.shareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix)
        SharePatchAllTask.shareHourStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix)
        //页面分享
        SharePatchAllTask.pageShareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix)
        //用户分享
        SharePatchAllTask.userShareDailyStat(baseRedisKey, taskId, dateStr, records_rdd_userShare_click, records_rdd_userShare_status, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix)
        //退出
        if (rddAndExit._2) {
          break()
        }
      }
    }
  }
}
