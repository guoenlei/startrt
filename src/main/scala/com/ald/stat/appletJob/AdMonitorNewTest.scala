package com.ald.stat.appletJob

import java.util
import java.util.{Date, Properties}

import com.ald.stat.appletJob.task.AdMonitorNewTask
import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object AdMonitorNewTest extends AbstractBaseJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)

  val prefix = "jobNewAdMonitor"
  val offsetPrefix = "newAdMonitor_offset"
  val baseRedisKey = "rt"

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    //    var partitionNumber = 20
    //    if (args.length >= 1) {
    //      if (args(0).toInt > 0) {
    //        partitionNumber = args(0).toInt
    //      }
    //    }

    // TODO change hbase name to newADmonitor and change to config file
    //    val newUserHbaseColumn = ConfigUtils.getProperty("hbase.user.column." + prefix)
    //    val newUserTableName = ConfigUtils.getProperty("hbase.user.table." + prefix)
    //    val linkAuthColumn = ConfigUtils.getProperty("hbase.auth.column." + prefix)
    //    val authUserTableName = ConfigUtils.getProperty("hbase.auth.table." + prefix)
    val newUserHbaseColumn = "newuser"
    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID_TEST" //测试
    //    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID"
    val authUserTableName = "STAT.ALDSTAT_ADVERTISE_AUTH_TEST" //测试
    //    val authUserTableName = "STAT.ALDSTAT_ADVERTISE_AUTH"
    val linkAuthColumn = "authStatus"
    //    val amountAuthColumn = "amountStatus"


    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
    //      .set("spark.streaming.kafka.consumer.cache.enabled", "false")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(60 * 60))

    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()

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
    //    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    //    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    //
    //    val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
    //    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, offsetPrefix)

    /** 维护最小和最大的offset */
    //    stream.foreachRDD(rdd => {
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd.foreachPartition(par => {
    //        val dateStr = ComputeTimeUtils.getDayAndHourStr(new Date())
    //        val offsetRedisCache = CacheRedisFactory.getInstances(offsetPrefix).asInstanceOf[AbstractRedisCache]
    //        val resource_offset = offsetRedisCache.getResource
    //        try {
    //          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    //          RddUtils.checkAndSaveMinOffsetNew(dateStr, o.topic, group, o.fromOffset, o.partition, resource_offset)
    //          RddUtils.checkAndSaveLatestOffsetNew(dateStr, o.topic, group, o.untilOffset, o.partition, resource_offset)
    //        } finally {
    //          if (resource_offset != null) resource_offset.close()
    //          if (offsetRedisCache != null) offsetRedisCache.close()
    //        }
    //      })
    //    })

    ArgsTool.analysisArgs(args)
    val day_wz = ArgsTool.day
    val ak = ArgsTool.ak
    val logs = ArgsTool.getDailyDataFrameWithFilter(sparkSession, "hdfs://10.0.100.17:4007/ald_log_parquet")
      .filter(_.contains(ak))

    // 对rdd中的json进行 解析
    val transform_stream = logs.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(record => {
        val logRecord = LogRecord.line2Bean(record)
        if (logRecord != null) {
          //          if(logRecord.op != "useless_openid"){
          recordsRdd += logRecord
          //          }
        }
      })
      recordsRdd.iterator
    })

//    println("***********************transform_stream : Count is " + transform_stream.count() + "*************************")

    // 定义时间。dateStr：redis-key的一部分，dateLong：上报时间校正，currentTime：时间戳，存成hbase的value
    val dateStr = ComputeTimeUtils.getDateStr(new Date())
    val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
    val currentTime = new Date().getTime

    // 业务操作。过滤出两个RDD，分别为link和全量的RDD。并对这两个RDD标记新用户，最终做细粒度业务操作
    //    transform_stream.foreachRDD(rdd => {
    val logRecordRdd_1: _root_.org.apache.spark.rdd.RDD[_root_.com.ald.stat.log.LogRecord] = convertLinkRDD(dateLong, transform_stream)
//    println("***********************logRecordRdd_1 : Count is " + logRecordRdd_1.count() + "*************************")

    val logRecordRdd_2 = convertTotalRDD(dateLong, transform_stream)
//    println("***********************logRecordRdd_2 : Count is " + logRecordRdd_2.count() + "*************************")

    transform_stream.unpersist()
    //对link和total分别标记新用户
    val logRecordRdd_link = handleNewUserForNewAdMonitorOnline(logRecordRdd_1, currentTime, newUserHbaseColumn, newUserTableName)
//    println("***********************logRecordRdd_link : Count is " + logRecordRdd_link.count() + "*************************")

    val logRecordRdd_total = handleNewUserForNewAdMonitorOnline(logRecordRdd_2, currentTime, newUserHbaseColumn, newUserTableName)
//    println("***********************logRecordRdd_total : Count is " + logRecordRdd_total.count() + "*************************")

    // 细粒度业务逻辑
    AdMonitorNewTask.allstat_first(dateStr, baseRedisKey, redisPrefix, logRecordRdd_link, logRecordRdd_total, kafkaProducer, ssc,
      currentTime, newUserTableName, newUserHbaseColumn, authUserTableName, linkAuthColumn)


    ssc.start()
    ssc.stop(false)
    ssc.stop()
//    ssc.awaitTermination()
  }

  /**
   * logRecordRdd_link：用于计算link的访问人数和新用户
   *
   * @param dateLong 时间校正
   * @param rdd
   * @return
   */
  private def convertLinkRDD(dateLong: Long, rdd: RDD[LogRecord]) = {
    val logRecordRdd_link = rdd.mapPartitions(par => {
      // 将字典表查询到的映射信息添加到linkMap中。<ak+linkkey : media_id>
      val linkMap = new util.HashMap[String, String]()
      use(getConnection()) { conn =>
        use(conn.createStatement()) {
          statement =>
            val rs = statement.executeQuery(
              """
                |select app_key,link_key,media_id
                |from ald_link_trace
                |where is_del=0
              """.
                stripMargin)
            while (rs.next()) {
              val app_key_and_link_key = rs.getString(1) + ":" + rs.getString(2)
              val media_id = rs.getInt(3)
              if (app_key_and_link_key != null && media_id != null) {
                linkMap.put(app_key_and_link_key.toString, media_id.toString)
              }
            }
        }
      }
      // 业务过滤、赋值、替换
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        //            val pair = JSON.parseObject(line)
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.at) &&
          StringUtils.isNotBlank(logRecord.et) &&
          StringUtils.isNotBlank(logRecord.dr) &&
          StrUtils.isInt(logRecord.dr) &&
          StringUtils.isNotBlank(logRecord.pp) &&
          logRecord.pp != "null" &&
          StringUtils.isNotBlank(logRecord.ifo) &&
          StringUtils.isNotBlank(logRecord.img) &&
          StringUtils.isNotBlank(logRecord.scene) && //用scene替换 position_id
          StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
          StringUtils.isNotBlank(logRecord.wsr_query_ald_media_id)
        ) {
          logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
          // 如果“当前页面”为空，则添加默认值：“000”
          if (StringUtils.isBlank(logRecord.pp)) {
            logRecord.pp = "000"
          }
          // 将media_id赋值为字典表查出来的值，或者空。
          // 为空过滤；如果不为空，给Json中的wsr_query_ald_me dia_id，wsr_query_ald_position_id赋值。
          val media_id = linkMap.get(logRecord.ak + ":" + logRecord.wsr_query_ald_link_key.trim)
          if (StringUtils.isNotBlank(media_id)) {
            logRecord.wsr_query_ald_media_id = media_id
            //用场景值替换，position_id
            if (logRecord.scene == "1058" ||
              logRecord.scene == "1035" ||
              logRecord.scene == "1014" ||
              logRecord.scene == "1038") {
              logRecord.wsr_query_ald_position_id = logRecord.scene
            } else {
              logRecord.wsr_query_ald_position_id = "其它"
            }
            recordsRdd += logRecord
          }
        }
      })
      recordsRdd.iterator
    })

    logRecordRdd_link
    }
    .persist(StorageLevel.MEMORY_AND_DISK)


  /**
   * logRecordRdd_total：用于计算小程序总的访问人数和新用户
   *
   * @param dateLong
   * @param rdd
   * @return
   */
  private def convertTotalRDD(dateLong: Long, rdd: RDD[LogRecord]) = {
    rdd.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.op) &&
          StringUtils.isNotBlank(logRecord.et)
        ) {
          logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
          // 如果“当前页面”为空，则添加默认值：“000”
          if (StringUtils.isBlank(logRecord.pp)) {
            logRecord.pp = "000"
          }
          recordsRdd += logRecord
        }
      })
      recordsRdd.iterator
    })
    //      .cache()
  }

}