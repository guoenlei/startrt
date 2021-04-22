package com.ald.stat.appletJob

import java.util.{Date, Properties}

import com.ald.stat.appletJob.task.{DailyAnalysisTrendTask, HourlyAnalysisTrendTask}
import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object AnalysisTrend extends AbstractBaseJob {

  val prefix = "default"
  val offsetPrefix = "trend_offset"
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
      //                  .set("spark.executor.cores", "2")
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


    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")


//    val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, offsetPrefix)

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
    val stream_rdd = stream.repartition(partitionNumber).cache()
    stream_rdd.foreachRDD(rdd=>{
      handleNewUserForOldSDK(rdd, baseRedisKey, prefix)
    })

    /**业务操作*/
    stream_rdd.foreachRDD(rdd => {
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
        val resource = redisCache.getResource

        val recordsRdd = ArrayBuffer[LogRecord]()
        try {
          par.foreach(line => {
            //RddUtils.checkAndSaveOffset(dateStr, line.topic(), group, line, resource_offset)
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.at) &&
              StringUtils.isNotBlank(logRecord.ev) &&
              StringUtils.isNotBlank(logRecord.uu) &&
              StringUtils.isNotBlank(logRecord.et) &&
              StringUtils.isNotBlank(logRecord.dr) &&
              StringUtils.isNotBlank(logRecord.pp)&&
              StringUtils.isNotBlank(logRecord.te)&&
              StrUtils.isInt(logRecord.dr)) {
              logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
              if (logRecord.v != null && SDKVersionUtils.isOldSDk(logRecord.v)) {
                markNewUser(logRecord, resource, baseRedisKey)
              }
              //如果是7.0.0以上的版本，则去除掉ev=app的数据，避免多算新用户数
              if(logRecord.v >= "7.0.0"){
                if(logRecord.ev != "app"){
                  recordsRdd += logRecord
                }
              }else{
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

      val grap_map = cacheGrapUser("online_status") //缓存灰度用户上线状态
      val isOnLine = isOnline("online_status")
      //趋势分析
      HourlyAnalysisTrendTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
      DailyAnalysisTrendTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
      logRecordRdd.unpersist(false)
      rdd.unpersist(false)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
