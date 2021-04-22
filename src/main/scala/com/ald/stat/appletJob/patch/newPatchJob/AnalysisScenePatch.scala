package com.ald.stat.appletJob.patch.newPatchJob

import java.util
import java.util.{Date, Properties}

import com.ald.stat.appletJob.AbstractBaseJob
import com.ald.stat.appletJob.patch.newPatchTask.ScenePatchAllTask
import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object AnalysisScenePatch extends AbstractBaseJob {

  val prefix = "jobScene"
  val offsetPrefix = "scene_offset"
  val baseRedisKey = "rt";

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    var partitionNumber = 20
    var patchHour=""
    var dateStr = ComputeTimeUtils.getDateStr(new Date())
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
        if(args(1).toInt > 9){
          patchHour = args(1)
        }else{
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
      .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
    rddPatch(sparkConf, taskId, dateStr, partitionNumber, patchType,patchHour)
  }

  /**
    *
    * @param sparkConf
    * @param partitionNumber
    */
  def rddPatch(sparkConf: SparkConf, taskId: String, dateStr: String, partitionNumber: Int, patchType: String,patchHour:String): Unit = {

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
        handleNewUserForOldSDK(re_rdd, baseRedisKey, prefix)
        //其他的分组ID
        val otherSceneId = ConfigUtils.getProperty("other.scene.id")
        val otherSceneGroupId = ConfigUtils.getProperty("other.scene.group.id")
        val unknownSceneGroupId = ConfigUtils.getProperty("unknown.scene.id")
        val unknownSceneId = ConfigUtils.getProperty("unknown.scene.group.id")

        val logRecordRdd: RDD[LogRecord] = re_rdd.mapPartitions(par => {
          val dateStr = ComputeTimeUtils.getDateStr(new Date())
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val sceneMap = new util.HashMap[String, String]()
          use(getConnection()) { conn =>
            use(conn.createStatement()) {
              statement =>
                val rs = statement.executeQuery(
                  """
                    |select sid,scene_group_id
                    |from ald_cms_scene
                  """.stripMargin)
                while (rs.next()) {
                  val sid = rs.getString(1)
                  val group_id = rs.getInt(2)
                  if (sid != null && group_id != null) {
                    sceneMap.put(sid.toString, group_id.toString)
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
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.dr) &&
                StringUtils.isNotBlank(logRecord.pp) &&
                StringUtils.isNotBlank(logRecord.scene)
              ) {
                logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
                //把新用户的会话中的所有记录都标记为ifo=true
                markNewUser(logRecord, resource, baseRedisKey)

                if (StringUtils.isBlank(logRecord.scene)) {
                  logRecord.scene = unknownSceneId
                  logRecord.scene_group_id = unknownSceneGroupId
                } else {
                  val sgid = sceneMap.get(logRecord.scene.trim)
                  if (StringUtils.isNotBlank(sgid)) {
                    logRecord.scene_group_id = sgid
                  } else {
                    //其他
                    logRecord.scene = otherSceneId
                    logRecord.scene_group_id = otherSceneGroupId
                  }
                }
                recordsRdd += logRecord
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

        //趋势分析
        val grap_map = cacheGrapUser("online_status_scene") //缓存灰度用户上线状态
        val isOnLine = isOnline("online_status_scene")

        //趋势分析
        ScenePatchAllTask.hourlyGroupStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix) //场景值组，分时统计
        ScenePatchAllTask.dailyGroupStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix) //场景值组，每日统计
        ScenePatchAllTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix) //场景值，分时统计
        ScenePatchAllTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix, patchHour, prefix) //场景值，每日统计
        //退出
        if (rddAndExit._2) {
          break()
        }
      }
    }
  }
}