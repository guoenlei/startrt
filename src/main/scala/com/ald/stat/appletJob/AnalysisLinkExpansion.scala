package com.ald.stat.appletJob

import java.util
import java.util.{Date, Properties}

import com.ald.stat.appletJob.offline.task.LinkExpansionTaskNew
import com.ald.stat.cache.{AbstractRedisCache, CacheClientFactory, CacheRedisFactory, ClientRedisCache}
import com.ald.stat.appletJob.task._
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getSplitConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object AnalysisLinkExpansion extends AbstractBaseJob {

  val logger = LoggerFactory.getLogger("AnalysisLinkNew")

  val prefix = "jobLinkNew"
  val offsetPrefix = "new_link_offset"
  val baseRedisKey = "rt"

  /**
   *
   * @param args
   */
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
      //      .setMaster(ConfigUtils.getProperty("spark.master.host"))
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

    //init scene meta data to redis
    //??????
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")

    val stream = KafkaConsume.getStream(ssc, group, topic) //?????????????????????Dstram
    //    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, offsetPrefix)

    /** ????????????????????????offset */
    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(par => {
        val dateStr = ComputeTimeUtils.getDayAndHourStr(new Date())
        val offsetRedisCache = CacheRedisFactory.getInstances(offsetPrefix).asInstanceOf[AbstractRedisCache]
        val resource_offset = offsetRedisCache.getResource
        try {
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)

          RddUtils.checkAndSaveMinOffsetNew(dateStr, o.topic, group, o.fromOffset, o.partition, resource_offset)
          RddUtils.checkAndSaveLatestOffsetNew(dateStr, o.topic, group, o.untilOffset, o.partition, resource_offset)

        } finally {
          if (resource_offset != null) resource_offset.close()
          if (offsetRedisCache != null) offsetRedisCache.close()
        }
      })
    })

    /** ???????????????????????? */
    val stream_rdd = stream.repartition(partitionNumber).cache()
    stream_rdd.foreachRDD(rdd => {
      handleNewUserForOldSDK(rdd, baseRedisKey, prefix)
    })
    //??????openId
    stream_rdd.foreachRDD(rdd => {
      handleOpenId(rdd, baseRedisKey, prefix)
    })


    /** ???????????? */
    stream_rdd.foreachRDD(rdd => {
      val coonMap = DBUtils.getSplitDBTuple()
      val sql = "select app_key,link_key,media_id from ald_link_trace where is_del = 0"
      val linkMap = DBUtils.readQRMapFromMysqlSplit(coonMap, sql)
      val boMap = ssc.sparkContext.broadcast(linkMap)
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
        val resource = redisCache.getResource
        val recordsRdd = ArrayBuffer[LogRecord]()
        try {
          par.foreach(line => {
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.at) &&
              StringUtils.isNotBlank(logRecord.ev) &&
              StringUtils.isNotBlank(logRecord.uu) &&
              StringUtils.isNotBlank(logRecord.et) &&
              StringUtils.isNotBlank(logRecord.dr) &&
              StrUtils.isInt(logRecord.dr) &&
              StringUtils.isNotBlank(logRecord.pp) &&
              StringUtils.isNotBlank(logRecord.img) &&
              StringUtils.isNotBlank(logRecord.ifo) &&
              StringUtils.isNotBlank(logRecord.scene) && //???scene?????? position_id
              StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
              StringUtils.isNotBlank(logRecord.wsr_query_ald_media_id)
            ) {
              logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //????????????
              //???????????????????????????????????????????????????ifo=true
              markNewUser(logRecord, resource, baseRedisKey)

              //???????????????????????????????????????????????????op=redis????????????openId
              markOpenId(logRecord, resource, baseRedisKey)

              val media_id = boMap.value.get(logRecord.ak + ":" + logRecord.wsr_query_ald_link_key.trim)
              if (StringUtils.isNotBlank(media_id)) {
                logRecord.wsr_query_ald_media_id = media_id
                //?????????????????????position_id
                if (logRecord.scene == "1058" ||
                  logRecord.scene == "1035" ||
                  logRecord.scene == "1014" ||
                  logRecord.scene == "1038") {
                  logRecord.wsr_query_ald_position_id = logRecord.scene
                } else {
                  logRecord.wsr_query_ald_position_id = "??????"
                }

                if (logRecord.v >= "7.0.0") {
                  if (logRecord.ev != "app") {
                    recordsRdd += logRecord
                  }
                } else {
                  recordsRdd += logRecord
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
        recordsRdd.iterator
      }).cache()

      val grap_map = cacheGrapUser("online_status_link") //??????????????????????????????
      val isOnLine = isOnline("online_status_link")
      LinkExpansionTaskNew.allStat_first(baseRedisKey, taskId, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, ssc)
      boMap.destroy()
      logRecordRdd.unpersist(false)
      rdd.unpersist(false)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
