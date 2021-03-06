package com.ald.stat.appletJob

import java.util
import java.util.{Date, Properties}

import com.ald.stat.appletJob.AnalysisTrend.{cacheGrapUser, handleNewUserForOldSDK, isOnline, markNewUser}
import com.ald.stat.appletJob.task._
import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink, RddUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * ζΊεεζ
  */
@Deprecated
object AnalysisPhoneModel {

  val prefix = "jobPhone"
  val offsetPrefix = "offset"
  lazy val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
  lazy val offsetRedisCache = CacheRedisFactory.getInstances(offsetPrefix).asInstanceOf[AbstractRedisCache]
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

    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, prefix)
    //ε­ε¨ζ°η¨ζ·ζ θ?°
    handleNewUserForOldSDK(stream, baseRedisKey, prefix)

    stream.repartition(partitionNumber).foreachRDD(rdd => {
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val brandMap = new util.HashMap[String, String]() //εη
        val modelMap = new util.HashMap[String, String]() //εε·
        use(getConnection()) { conn =>
          use(conn.createStatement()) {
            statement =>
              val rs = statement.executeQuery(
                """
                  |select uname,brand,name
                  |from phone_model
                """.
                  stripMargin)
              while (rs.next()) {
                val uname = rs.getString(1)
                val brand = rs.getString(2)
                val name = rs.getString(3)
                if (uname != null && brand != null) {
                  brandMap.put(uname.toString, brand.toString)
                }
                if (uname != null && name != null) {
                  modelMap.put(uname.toString, name.toString)
                }
              }
          }
        }

        val recordsRdd = ArrayBuffer[LogRecord]()
        use(redisCache.getResource) { resource =>
          use(offsetRedisCache.getResource) { resource_offset =>
            par.foreach(line => {
              RddUtils.checkAndSaveOffset(dateStr, line.topic(), group, line, resource_offset)
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.dr) &&
                StringUtils.isNotBlank(logRecord.pp) &&
                StringUtils.isNotBlank(logRecord.pm)) {
                if (logRecord.v != null && logRecord.v < "7.0.0") {
                  markNewUser(logRecord, resource, baseRedisKey)
                }

                val brand = brandMap.get(logRecord.pm.trim) //εη
                if (StringUtils.isNotBlank(brand)) {
                  logRecord.brand = brand
                } else {
                  logRecord.brand = "ζͺη₯" //ε¦ζδΈε­ε¨εζεηεΌθ΅εΌδΈΊ"ζͺη₯"
                }
                val model = resource.get(logRecord.pm.trim) //ζΊε
                if (StringUtils.isNotBlank(model)) {
                  logRecord.model = model
                } else {
                  logRecord.model = "ζͺη₯" //ε¦ζδΈε­ε¨εζζΊεεΌθ΅εΌδΈΊ"ζͺη₯"
                }
                recordsRdd += logRecord
              }
            })
          }
        }
        recordsRdd.iterator
      }).cache()

      val grap_map = cacheGrapUser("online_status_phone") //ηΌε­η°εΊ¦η¨ζ·δΈηΊΏηΆζ
      val isOnLine = isOnline()

      DailyModelTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine,redisPrefix) //ζΊε
      DailyBrandTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix) //εη
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
