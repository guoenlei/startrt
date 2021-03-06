package com.ald.stat.component.stat

import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait PV extends StatBase {

  //  lazy val redisCache = CacheRedisFactory.getInstances(Redis_Prefix).asInstanceOf[AbstractRedisCache]
  val name = "PV"

  def stat[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering](statTrait: KeyTrait[C, K], logRecords: RDD[C]): RDD[(K, Long)] = {

    logRecords.filter(record => record.ev == "page").map(record => (statTrait.getKey(record), 1l)).reduceByKey((x, y) => (x + y))
  }

  def statIncreaseCache[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering](baseRedisKey: String, dateStr: String, taskId: String, statTrait: KeyTrait[C, K], rddRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(K, Long)] = {
    rddRecords.mapPartitions(
      par => {
        val resource = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache].getResource
        val csts = ArrayBuffer[(K, Long)]()
        val redisKey = getKey(dateStr, baseRedisKey)
        par.foreach(record => {
          val baseKey = statTrait.getKey(record)
          if (record.ev == "page") {
            csts.+=((baseKey, 0l))
          } else {
            resource.hincrBy(redisKey, baseKey.hashKey, 1l)
            csts.+=((baseKey, 1l))
          }
        })
        resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        try {
          if (resource != null)
            resource.close()
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }
        csts.iterator
      }).reduceByKey((x, y) => (x + y))
  }

  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:$name"
  }
}
