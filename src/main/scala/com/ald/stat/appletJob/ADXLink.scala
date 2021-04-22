package com.ald.stat.appletJob

import java.util
import java.util.{Date, Properties}

import com.ald.stat.appletJob.task._
import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object ADXLink extends AbstractBaseJob {

  val logger = LoggerFactory.getLogger("ADXLink")

  val prefix = "jobAdxLink"
  val offsetPrefix = "adx_link_offset"
  val baseRedisKey = "rt";

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

    //init scene meta data to redis
    //广播
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")

//        val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, offsetPrefix)

    /** 维护最小和最大的offset */
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

    /** 分区并标记新用户 */
    val stream_rdd = stream.repartition(partitionNumber).cache()
    stream_rdd.foreachRDD(rdd => {
      handleNewUserForOldSDK(rdd, baseRedisKey, prefix)
    })


    /** 业务操作 */
    stream_rdd.foreachRDD(rdd => {
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val linkMap = new util.HashMap[String, (String, String)]()
        val mediaMap = new util.HashMap[String, String]()
        use(getConnection()) { conn =>
          use(conn.createStatement()) {
            statement =>
              val rs = statement.executeQuery(
                """
                  |select link_key,media_id,link_name
                  |from ald_link_trace
                  |where is_del=0
                """.
                  stripMargin)
              while (rs.next()) {
                val link_key = rs.getString(1)
                val media_id = rs.getInt(2)
                val link_name = rs.getString(3)
                if (link_key != null && media_id != null && link_name != null) {
                  linkMap.put(link_key.toString, (media_id.toString, link_name.toString))
                }
              }
              val rs2 = statement.executeQuery(
                """
                  |select media_id,media_name
                  |from ald_media
                """.
                  stripMargin)
              while (rs2.next()) {
                val media_id = rs2.getString(1)
                val media_name = rs2.getString(2)
                if (media_id != null && media_name != null) {
                  mediaMap.put(media_id.toString, media_name.toString)
                }
              }
          }
        }

        val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
        val resource = redisCache.getResource
        val recordsRdd = ArrayBuffer[LogRecord]()
        try {
          par.foreach(line => {
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              logRecord.op != "useless_openid" &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.et) &&
              StringUtils.isNotBlank(logRecord.img) &&
              StringUtils.isNotBlank(logRecord.ifo) &&
              StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
              StringUtils.isNotBlank(logRecord.wsr_query_ald_media_id)
            ) {
              logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
              //把新用户的会话中的所有记录都标记为ifo=true
              markNewUser(logRecord, resource, baseRedisKey)
              var link_name = "未知活动"
              if (linkMap.get(logRecord.wsr_query_ald_link_key.trim) != null) {
                link_name = linkMap.get(logRecord.wsr_query_ald_link_key.trim)._2
              }
              logRecord.wsr_query_ald_link_name = link_name
              var media_id = ""
              if (linkMap.get(logRecord.wsr_query_ald_link_key.trim) != null) {
                media_id = linkMap.get(logRecord.wsr_query_ald_link_key.trim)._1
                if (StringUtils.isNotBlank(media_id)) {
                  val media_name = mediaMap.get(media_id.trim)
                  if (StringUtils.isNotBlank(media_name)) {
                    logRecord.wsr_query_ald_media_id = media_name
                    recordsRdd += logRecord
                  } else {
                    logRecord.wsr_query_ald_media_id = "未知渠道"
                    recordsRdd += logRecord
                  }
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
        HourlyADXlinkTask.adxLinkStat(logRecordRdd, kafkaProducer, prefix)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
