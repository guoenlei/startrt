package com.ald.stat.component.stat

import com.ald.stat.cache.{AbstractRedisCache, BaseRedisCache, CacheRedisFactory}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
import com.ald.stat.component.session.{SessionBase, SessionSum, SessionTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}
import org.apache.commons.beanutils.BeanUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

trait SessionStat extends StatBase {

  val logger = LoggerFactory.getLogger(this.getClass)
  val name = "SESSION"

  /**
   * 旧版广告监测实时
   *
   * @param rdd
   * @param key
   * @param session
   * @tparam C
   * @tparam K
   * @tparam O
   * @return
   */
  def stat[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (rdd: RDD[C], key: KeyTrait[C, K], session: SessionTrait[C, O]): RDD[(K, O)] = {
    rdd.map(record => (key.getKey(record), session.getEntity(record)))
      .reduceByKey((r1, r2) => {
        mergeSession(r1, r2)
      })
  }

  /**
   * 新版广告监测实时
   * 合并会话dimensionKey的SessionBaseImpl
   * @param rdd
   * @param key
   * @param session
   * @tparam C
   * @tparam K
   * @tparam O
   * @return
   */
  def stat_2[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (rdd: RDD[C], key: KeyTrait[C, K], session: SessionTrait[C, O]): RDD[(K, O)] = {
    rdd.map(record => (key.getKey(record), session.getEntity(record)))
      .reduceByKey((r1, r2) => {
        mergeSession(r1, r2)
      })
  }

  /**
   * 旧版广告监测离线
   *
   * @param rdd
   * @param key
   * @param session
   * @tparam K
   * @tparam O
   * @return
   */
  def statOfflineLink[K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (rdd: RDD[LogRecord], key: KeyTrait[LogRecord, K], session: SessionTrait[LogRecord, O]): RDD[(K, O)] = {
    val finalRDD = rdd.map(record => {
      var recordList = ListBuffer[LogRecord]()
      val record1 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record2 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record3 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record4 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record5 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record6 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record7 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record8 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      record1.hour = "all"
      record1.wsr_query_ald_link_key = "all"
      record1.wsr_query_ald_media_id = "all"
      record1.wsr_query_ald_position_id = "all"
      recordList.+=(record1)
      record2.wsr_query_ald_link_key = "all"
      record2.wsr_query_ald_media_id = "all"
      record2.wsr_query_ald_position_id = "all"
      recordList.+=(record2)
      record3.hour = "all"
      record3.wsr_query_ald_media_id = "all"
      record3.wsr_query_ald_position_id = "all"
      recordList.+=(record3)
      record4.hour = "all"
      record4.wsr_query_ald_link_key = "all"
      record4.wsr_query_ald_position_id = "all"
      recordList.+=(record4)
      record5.wsr_query_ald_link_key = "all"
      record5.wsr_query_ald_position_id = "all"
      recordList.+=(record5)
      record6.wsr_query_ald_media_id = "all"
      record6.wsr_query_ald_position_id = "all"
      recordList.+=(record6)
      record7.hour = "all"
      record7.wsr_query_ald_link_key = "all"
      record7.wsr_query_ald_media_id = "all"
      recordList.+=(record7)
      record8.wsr_query_ald_link_key = "all"
      record8.wsr_query_ald_media_id = "all"
      recordList.+=(record8)
    }).flatMap(_.toList)
    finalRDD.map(record => (key.getKey(record), session.getEntity(record)))
      .reduceByKey((r1, r2) => {
        mergeSession(r1, r2)
      })
  }

  /**
   * 新版广告监测离线
   *
   * @param rdd
   * @param key
   * @param session
   * @tparam K
   * @tparam O
   * @return
   */
  def statOfflineLink_2[K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (rdd: RDD[LogRecord], key: KeyTrait[LogRecord, K], session: SessionTrait[LogRecord, O]): RDD[(K, O)] = {
    val finalRDD = rdd.map(record => {
      var recordList = ListBuffer[LogRecord]()
      val record1 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record2 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record3 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record4 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record5 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record6 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record7 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      val record8 = BeanUtils.cloneBean(record).asInstanceOf[LogRecord]
      // record1：daily full
      record1.hour = "all"
      record1.wsr_query_ald_link_key = "all"
      record1.wsr_query_ald_media_id = "all"
      record1.wsr_query_ald_position_id = "all"
      // record2：hour full
      recordList.+=(record1)
      record2.wsr_query_ald_link_key = "all"
      record2.wsr_query_ald_media_id = "all"
      record2.wsr_query_ald_position_id = "all"
      recordList.+=(record2)
      // record3：daily link
      record3.hour = "all"
      record3.wsr_query_ald_media_id = "all"
      record3.wsr_query_ald_position_id = "all"
      recordList.+=(record3)
      // record4：daily media
      record4.hour = "all"
      record4.wsr_query_ald_link_key = "all"
      record4.wsr_query_ald_position_id = "all"
      recordList.+=(record4)
      // record5：hour media
      record5.wsr_query_ald_link_key = "all"
      record5.wsr_query_ald_position_id = "all"
      recordList.+=(record5)
      // record6：hour link
      record6.wsr_query_ald_media_id = "all"
      record6.wsr_query_ald_position_id = "all"
      recordList.+=(record6)
      // record7：daily position
      record7.hour = "all"
      record7.wsr_query_ald_link_key = "all"
      record7.wsr_query_ald_media_id = "all"
      recordList.+=(record7)
      // record8：hour position
      record8.wsr_query_ald_link_key = "all"
      record8.wsr_query_ald_media_id = "all"
      recordList.+=(record8)
    }).flatMap(_.toList)
    finalRDD.map(record => (key.getKey(record), session.getEntity(record)))
      .reduceByKey((r1, r2) => {
        mergeSession(r1, r2)
      })
  }

  /**
   * 补偿计算session
   *
   * @param baseRedisKey
   * @param taskId
   * @param dateStr
   * @param rdd
   * @param k
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @tparam R
   * @return
   */
  def statPathSession[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, taskId: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, SessionSum)] = {
    rdd.mapPartitions(
      par => {
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        use(redisCache.getResource) {
          resource =>
            par.foreach(record => {
              arrayBuffer.+=(handlePatchSumSession(baseRedisKey, dateStr, resource, record, k))
            })
        }
        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
      use(redisCache.getResource) {
        resource =>
          val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
          par.foreach(
            record => {
              val sessionKey = record._1.hashKey
              val final_key = getFinalKey(cacheSessionKey, sessionKey) //获取最终的key
              resource.set(final_key, toSumSessionArray(record._2)) //直接用计算结果覆盖缓存中的结果
              resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              arrayBuffer.+=((record))
            }
          )
      }
      arrayBuffer.iterator
    })
  }

  /**
   * 获取补偿时的sumSession
   *
   * @param baseRedisKey
   * @param dateStr
   * @param resource
   * @param record
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @return
   */
  def handlePatchSumSession[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (baseRedisKey: String, dateStr: String, resource: BaseRedisCache, record: (K, O), k: KeyParentTrait[C, K, P]): (P, SessionSum) = {
    val redisKey = getSessionKey(dateStr, baseRedisKey)
    val baseKey = k.getBaseKey(record._1)
    val key = record._1.hashKey
    val final_key = getFinalKey(redisKey, key) //获取最终的key
    record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)
    if (resource.exists(final_key) == false) {
      resource.set(final_key, toSessionBaseArray(record._2))
      resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
    }
    (baseKey, generateSessionSum(record._2))
  }

  def statSum[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, P <: DimensionKey : ClassTag : Ordering]
  (rdd: RDD[(K, O)], statTrait: KeyParentTrait[C, K, P]): RDD[(P, SessionSum)] = {
    rdd.map((r) => {
      (statTrait.getBaseKey(r._1), generateSessionSum(r._2))
    }).reduceByKey((r1, r2) => {
      sumSessionSum(r1, r2, true)
    })
  }


  /**
   *
   * @param baseRedisKey
   * @param taskId 批次ID
   * @param dateStr
   * @param rdd
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @tparam R
   * @return
   */
  def doCacheWithRedisMark[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, taskId: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, (SessionSum, String))] = {

    rdd.mapPartitions(
      par => {
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        try {
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handleSumSession(baseRedisKey, dateStr, resource, record, k))
              })
              //设置失效时间
              val redisKey = getSessionKey(dateStr, baseRedisKey)
              resource.expireAt(redisKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }

        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (SessionSum, String))]()
      try {
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record._1.hashKey
                val cacheSessionSumStr = resource.hget(cacheSessionKey, sessionKey)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  val mergedSessionSum = sumSessionSum(sessionSum, record._2, true)
                  resource.hset(cacheSessionKey, sessionKey, toSumSessionArray(mergedSessionSum))
                  arrayBuffer.+=((record._1, (mergedSessionSum, name)))
                } else {
                  resource.hset(cacheSessionKey, sessionKey, toSumSessionArray(record._2))
                  arrayBuffer.+=((record._1, (record._2, name)))
                }
              }
            )
            resource.expireAt(cacheSessionKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }


  /**
   *
   * @param baseRedisKey
   * @param taskId 批次ID
   * @param dateStr
   * @param rdd
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @tparam R
   * @return
   */
  def doCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, taskId: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, SessionSum)] = {

    rdd.mapPartitions(
      par => {
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        try {
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handleSumSession(baseRedisKey, dateStr, resource, record, k))
              })
            //设置失效时间
            //val redisKey = getSessionKey(dateStr, baseRedisKey)
            //resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }

        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {

      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
      try {
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record._1.hashKey
                val final_key = getFinalKey(cacheSessionKey, sessionKey) //获取最终的key
                val cacheSessionSumStr = resource.get(final_key)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  val mergedSessionSum = sumSessionSum(sessionSum, record._2, true)
                  resource.set(final_key, toSumSessionArray(mergedSessionSum))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1, mergedSessionSum))
                } else {
                  resource.set(final_key, toSumSessionArray(record._2))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record))
                }
              }
            )
          //resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }

  /**
   * 旧版广告监测实时
   * 为了能够与uv和新增授权的rddjoin，所以将返回值的key转换成String类型
   *
   * @param baseRedisKey
   * @param dateStr
   * @param rdd
   * @param k
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @tparam R
   * @return
   */
  def doCacheStr[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(String, SessionSum)] = {

    rdd.mapPartitions(
      par => {
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        try {
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handleSumSession(baseRedisKey, dateStr, resource, record, k))
              })
            //设置失效时间
            //val redisKey = getSessionKey(dateStr, baseRedisKey)
            //resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }

        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {

      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(String, SessionSum)]()
      try {
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record._1.hashKey
                val final_key = getFinalKey(cacheSessionKey, sessionKey) //获取最终的key
                val cacheSessionSumStr = resource.get(final_key)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  val mergedSessionSum = sumSessionSum(sessionSum, record._2, true)
                  resource.set(final_key, toSumSessionArray(mergedSessionSum))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1.toString, mergedSessionSum))
                } else {
                  resource.set(final_key, toSumSessionArray(record._2))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1.toString, record._2))
                }
              }
            )
          //resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }

  /**
   * 新版广告监测实时
   * 为了能够与uv和新增授权的rddjoin，所以将返回值的key转换成String类型
   *
   * @param baseRedisKey
   * @param dateStr
   * @param rdd
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @tparam R
   * @return
   */
  def doCacheStr_2[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(String, (SessionSum, SessionSum))] = {

    rdd.mapPartitions(
      par => {
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        val arrayBuffer = ArrayBuffer[(P, (SessionSum, SessionSum))]()
        try {
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handleSumSessionLink(baseRedisKey, dateStr, resource, record, k))
              })
            //设置失效时间
            //val redisKey = getSessionKey(dateStr, baseRedisKey)
            //resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }

        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      //(与缓存中聚合后的sessionSum，当前批次聚合的sessionSum)
      (sumSessionSum(s1._1, s2._1, true), sumSessionSum(s1._2, s2._2, true))
    }).mapPartitions(par => {

      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(String, (SessionSum, SessionSum))]()
      try {
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record._1.hashKey
                val final_key = getFinalKey(cacheSessionKey, sessionKey) //获取最终的key
                val cacheSessionSumStr = resource.get(final_key)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  val mergedSessionSum = sumSessionSum(sessionSum, record._2._1, true)
                  resource.set(final_key, toSumSessionArray(mergedSessionSum))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1.toString, (mergedSessionSum, record._2._2)))
                } else {
                  resource.set(final_key, toSumSessionArray(record._2._1))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1.toString, (record._2._1, record._2._2)))
                }
              }
            )
          //resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }

  /**
   * 新、旧广告监测
   * 实时session的计算
   *
   * @param baseRedisKey
   * @param dateStr
   * @param rdd
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @tparam R
   * @return
   */
  def doCacheLink[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, (SessionSum, SessionSum))] = {

    rdd.mapPartitions(
      par => {
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        val arrayBuffer = ArrayBuffer[(P, (SessionSum, SessionSum))]()
        try {
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handleSumSessionLink(baseRedisKey, dateStr, resource, record, k))
              })
            //设置失效时间
            //val redisKey = getSessionKey(dateStr, baseRedisKey)
            //resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }

        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      //(与缓存中聚合后的sessionSum，当前批次聚合的sessionSum)
      (sumSessionSum(s1._1, s2._1, true), sumSessionSum(s1._2, s2._2, true))
    }).mapPartitions(par => {

      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (SessionSum, SessionSum))]()
      try {
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record._1.hashKey
                val final_key = getFinalKey(cacheSessionKey, sessionKey) //获取最终的key
                val cacheSessionSumStr = resource.get(final_key)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  val mergedSessionSum = sumSessionSum(sessionSum, record._2._1, true)
                  resource.set(final_key, toSumSessionArray(mergedSessionSum))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1, (mergedSessionSum, record._2._2)))
                } else {
                  resource.set(final_key, toSumSessionArray(record._2._1))
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1, (record._2._1, record._2._2)))
                }
              }
            )
          //resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }

  /**
   * 新、旧广告监测公用
   * 离线session的计算
   *
   * @param dateStr
   * @param rdd
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @tparam R
   * @return
   */
  def doCacheLinkOffline[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P]): RDD[(P, SessionSum)] = {

    rdd.mapPartitions(
      par => {
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        par.foreach(record => {
          //                val a = ((handleSumSessionLinkOffline(record，k)))
          arrayBuffer.+=(handleSumSessionLinkOffline(record, k))
        })
        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {
      val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
      par.foreach(
        record => {
          arrayBuffer.+=((record._1, record._2))
        }
      )
      arrayBuffer.iterator
    })
  }

  //计算扩张维度的session累加
  def increaseSession(sessionSum: SessionSum, baseRedisKey: String, dateStr: String, redisPrefix: Broadcast[String], key: String): SessionSum = {
    val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
    try {
      use(redisCache.getResource) {
        resource =>
          val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
          val sessionKey = HashUtils.getHash(key).toString
          val final_key = getFinalKey(cacheSessionKey, sessionKey) //获取最终的key
        val cacheSessionSumStr = resource.get(final_key)
          if (StringUtils.isNotBlank(cacheSessionSumStr)) {
            val sessionSumCache = toSumSession(cacheSessionSumStr.split(","))
            val mergedSessionSum = sumSessionSum(sessionSumCache, sessionSum, true)
            resource.set(final_key, toSumSessionArray(mergedSessionSum))
            resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
            mergedSessionSum
          } else {
            resource.set(final_key, toSumSessionArray(sessionSum))
            resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
            sessionSum
          }
        //resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
      }
    } finally {
      if (redisCache != null) redisCache.close()
    }
  }

  //计算扩张维度的session累加,用于离线计算
  def reduceSession(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], ssc: SparkSession, hour: String, wsr_query_ald_link_key: String,
                    wsr_query_ald_media_id: String, ag_ald_position_id: String): RDD[(String, SessionSum)] = {
    val arrayBuffer = ArrayBuffer[(String, SessionSum)]()
    for (i <- resultArray) {
      val splits = i._1.toString.split(":")
      var baseKey = ""
      if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + "all" + ":" + "all" + ":" + "all"
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + "all" + ":" + "all" + ":" + "all"
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + splits(3) + ":" + "all" + ":" + "all"
      } else if (hour != "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(3) + ":" + "all" + ":" + "all"
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + splits(3) + ":" + splits(4) + ":" + splits(5)
      } else if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + "all" + ":" + splits(4) + ":" + "all"
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + "all" + ":" + splits(4) + ":" + "all"
      } else if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + "all" + ":" + splits(4) + ":" + splits(5)
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + "all" + ":" + splits(4) + ":" + splits(5)
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + splits(3) + ":" + splits(4) + ":" + "all"
      } else if (hour != "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(3) + ":" + splits(4) + ":" + "all"
      } else if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + "all" + ":" + "all" + ":" + splits(5)
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + "all" + ":" + "all" + ":" + splits(5)
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + "all" + ":" + splits(3) + ":" + "all" + ":" + splits(5)
      } else if (hour != "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(3) + ":" + "all" + ":" + splits(5)
      }
      arrayBuffer.+=((baseKey, i._2._1._2))
    }
    val sessionRDD = ssc.sparkContext.parallelize(arrayBuffer)
    val finalRDD = sessionRDD.reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    })
    finalRDD
  }


  //计算扩张维度的session累加,用于实时计算
  def reduceSessionOnline(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], ssc: StreamingContext,
                          hour: String, wsr_query_ald_link_key: String, wsr_query_ald_media_id: String, ag_ald_position_id: String,
                          redisPrefix: Broadcast[String], dateStr: String, baseRedisKey: String): RDD[(String, SessionSum)] = {
    val arrayBuffer = ArrayBuffer[(String, SessionSum)]()
    for (i <- resultArray) {
      val splits = i._1.toString.split(":")
      var baseKey = ""
      if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1)
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2)
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(3)
      } else if (hour != "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(3)
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(3) + ":" + splits(4) + ":" + splits(5)
      } else if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(4)
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(4)
      } else if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(4) + ":" + splits(5)
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(4) + ":" + splits(5)
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(3) + ":" + splits(4)
      } else if (hour != "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id != "all" && ag_ald_position_id == "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(3) + ":" + splits(4)
      } else if (hour == "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(5)
      } else if (hour != "all" && wsr_query_ald_link_key == "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(5)
      } else if (hour == "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(3) + ":" + splits(5)
      } else if (hour != "all" && wsr_query_ald_link_key != "all" && wsr_query_ald_media_id == "all" && ag_ald_position_id != "all") {
        baseKey = splits(0) + ":" + splits(1) + ":" + splits(2) + ":" + splits(3) + ":" + splits(5)
      }
      arrayBuffer.+=((baseKey, i._2._1._2))
    }
    val sessionRDD = ssc.sparkContext.parallelize(arrayBuffer)
    val finalRDD = sessionRDD.reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {

      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(String, SessionSum)]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(record => {
              val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
              val sessionKey = HashUtils.getHash(record._1).toString
              val final_key = getFinalKey(cacheSessionKey, sessionKey) //获取最终的key
              val cacheSessionSumStr = resource.get(final_key)
              if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                val sessionSumCache = toSumSession(cacheSessionSumStr.split(","))
                val mergedSessionSum = sumSessionSum(sessionSumCache, record._2, true)
                resource.set(final_key, toSumSessionArray(mergedSessionSum))
                resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                arrayBuffer.+=((record._1, mergedSessionSum))
              } else {
                resource.set(final_key, toSumSessionArray(record._2))
                resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                arrayBuffer.+=((record._1, record._2))
              }
            })
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      arrayBuffer.iterator
    })
    finalRDD
  }

  /**
   * 用于外链的扩张部分的累加算子，
   * 返回：与redis聚合后的sessionSum 和 当前批次的BatchSessionSum
   *
   * @param baseRedisKey
   * @param dateStr
   * @param resource
   * @param record
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @return
   */
  def handleSumSessionLink[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (baseRedisKey: String, dateStr: String, resource: BaseRedisCache, record: (K, O), k: KeyParentTrait[C, K, P]): (P, (SessionSum, SessionSum)) = {

    //这里搞一个二元组
    val redisKey = getSessionKey(dateStr, baseRedisKey)
    val baseKey = k.getBaseKey(record._1)
    val key = record._1.hashKey
    val final_key = getFinalKey(redisKey, key) //获取最终的key
    val cacheSessionJson = resource.get(final_key) //平铺后，直接采用get方式获取
    if (StringUtils.isNotBlank(cacheSessionJson)) {
      try {
        val cacheSession = toSessionBase(cacheSessionJson.split(Sum_Delimiter))
        val oldSessionDuration = cacheSession.sessionDurationSum
        val mergedSessionBase = mergeSession(cacheSession, record._2) //当前批次与缓存中进行聚合

        resource.set(final_key, toSessionBaseArray(mergedSessionBase))
        resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期

        val sessionSum = generateSessionSum(mergedSessionBase)
        val batchSessionSum = generateSessionSum(record._2) //当前批次的sessionSum
        if (cacheSession.newUser == 0) {
          sessionSum.newUserCount = 0
          batchSessionSum.newUserCount = 0
        } //如果缓存中已经是新用户，则此时不能再进行新用户的计数了
        sessionSum.sessionCount = 0 //因为该会话已经在缓存中，存在，所以不能重复计数
        batchSessionSum.sessionCount = 0
        if (cacheSession.pages == 1) { //如果缓存中pages=1，则说明该会话已经被记录过一次跳出数
          if (mergedSessionBase.pages > 1) { //如果当前批次与缓存中聚合后，发现聚合后pages>1,则说明这个会话已经不是跳出的会话了
            sessionSum.onePageofSession = -1 //此时应当减去之前记录进去的跳出数
            batchSessionSum.onePageofSession = -1
          } else if (mergedSessionBase.pages == 1) { //如果当前批次与缓存中聚合后，pages依然为1，则说明该会话重复访问了一个页面
            sessionSum.onePageofSession = 0 //此时不再重复记录跳出数
            batchSessionSum.onePageofSession = 0
          }
        }
        sessionSum.sessionDurationSum = mergedSessionBase.sessionDurationSum - oldSessionDuration
        (baseKey, (sessionSum, batchSessionSum))
      }
      catch {
        case t: Throwable => {
          logger.error(s"convert exeception $cacheSessionJson", t)
          record._2.pages = 1
          record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)

          resource.set(final_key, toSessionBaseArray(record._2))
          resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          (baseKey, (generateSessionSum(record._2), (generateSessionSum(record._2))))
        }
      }
    } else {
      record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)

      resource.set(final_key, toSessionBaseArray(record._2))
      resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
      (baseKey, (generateSessionSum(record._2), (generateSessionSum(record._2))))
    }
  }

  /**
   * 用于外链的扩张部分的离线
   *
   * @param record
   * @param k
   * @tparam C
   * @tparam K
   * @tparam P
   * @tparam O
   * @return
   */
  def handleSumSessionLinkOffline[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (record: (K, O), k: KeyParentTrait[C, K, P]): (P, SessionSum) = {

    val baseKey = k.getBaseKey(record._1)
    record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)
    (baseKey, (generateSessionSum(record._2)))
  }

  def handleSumSession[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (baseRedisKey: String, dateStr: String, resource: BaseRedisCache, record: (K, O), k: KeyParentTrait[C, K, P]): (P, SessionSum) = {

    //这里搞一个二元组
    val redisKey = getSessionKey(dateStr, baseRedisKey)
    val baseKey = k.getBaseKey(record._1)
    val key = record._1.hashKey
    val final_key = getFinalKey(redisKey, key) //获取最终的key
    val cacheSessionJson = resource.get(final_key) //平铺后，直接采用get方式获取
    if (StringUtils.isNotBlank(cacheSessionJson)) {
      try {
        val cacheSession = toSessionBase(cacheSessionJson.split(Sum_Delimiter))
        val oldSessionDuration = cacheSession.sessionDurationSum
        val mergedSessionBase = mergeSession(cacheSession, record._2) //当前批次与缓存中进行聚合

        resource.set(final_key, toSessionBaseArray(mergedSessionBase))
        resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期

        val sessionSum = generateSessionSum(mergedSessionBase)
        if (cacheSession.newUser == 0) sessionSum.newUserCount = 0 //如果缓存中已经是新用户，则此时不能再进行新用户的计数了
        sessionSum.sessionCount = 0 //因为该会话已经在缓存中，存在，所以不能重复计数

        if (cacheSession.pages == 1) { //如果缓存中pages=1，则说明该会话已经被记录过一次跳出数
          if (mergedSessionBase.pages > 1) { //如果当前批次与缓存中聚合后，发现聚合后pages>1,则说明这个会话已经不是跳出的会话了
            sessionSum.onePageofSession = -1 //此时应当减去之前记录进去的跳出数
          } else if (mergedSessionBase.pages == 1) { //如果当前批次与缓存中聚合后，pages依然为1，则说明该会话重复访问了一个页面
            sessionSum.onePageofSession = 0 //此时不再重复记录跳出数
          }
        }

        sessionSum.sessionDurationSum = mergedSessionBase.sessionDurationSum - oldSessionDuration
        (baseKey, sessionSum)
      }
      catch {
        case t: Throwable => {
          logger.error(s"convert exeception $cacheSessionJson", t)
          record._2.pages = 1
          record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)

          resource.set(final_key, toSessionBaseArray(record._2))
          resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期

          (baseKey, generateSessionSum(record._2))
        }
      }
    }
    else {
      record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)

      resource.set(final_key, toSessionBaseArray(record._2))
      resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期

      (baseKey, generateSessionSum(record._2))
    }
  }

  /**
   * 直接读取已经存储在redis key 的全量数据
   *
   * @param keys         DimensionKey
   * @param dateStr      20180101
   * @param baseRedisKey redis key prefix
   * @param redisPrefix  模块redis prefix
   * @tparam P 范型
   * @return
   */
  def getCachedKeysStat[P <: DimensionKey : ClassTag : Ordering](keys: RDD[P], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): RDD[(P, SessionSum)] = {
    keys.mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
      try {
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record.hashKey
                val cacheSessionSumStr = resource.hget(cacheSessionKey, sessionKey)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  arrayBuffer.+=((record, sessionSum))
                }
              }
            )
            resource.expireAt(cacheSessionKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }


  def toSessionBase(v: Array[String]): SessionBase = {
    val o = new SessionBase
    o.newUser = v(0).toInt
    o.pages = v(1).toInt
    o.url = v(2)
    o.startDate = v(3).toLong
    o.endDate = v(4).toLong
    o.firstUrl = v(5)
    o.ev = v(6)
    try {
      o.sessionDurationSum = v(7).toLong
    } catch {
      case _: NumberFormatException => {
        o.sessionDurationSum = 1
      }
    }
    o
  }

  def toSessionBaseArray(o: SessionBase): String = {
    Array(o.newUser, o.pages, o.url, o.startDate, o.endDate, o.firstUrl, o.ev, o.sessionDurationSum).mkString(Sum_Delimiter)
  }

  def generateSessionSum(sessionBase: SessionBase): SessionSum = {
    val cs = new SessionSum //将数据转成下一步要计算的内容
    if (sessionBase.newUser == 0)
      cs.newUserCount = 1 //新用户
    if (sessionBase.pages == 1)
      cs.onePageofSession = 1
    else
      cs.onePageofSession = 0
    cs.sessionCount = 1
    cs.sessionDurationSum = sessionBase.sessionDurationSum
    cs.pagesSum = sessionBase.pages
    cs
  }

  def toSumSession(v: Array[String]): SessionSum = {
    val o = new SessionSum
    o.pvs = v(0).toLong
    o.uvs = v(1).toLong
    o.ips = v(2).toLong
    o.pagesSum = v(3).toLong
    o.onePageofSession = v(4).toLong
    o.sessionDurationSum = v(5).toLong
    o.sessionCount = v(6).toLong
    o.newUserCount = v(7).toLong
    o
  }

  def toSumSessionArray(o: SessionSum): String = {
    Array(o.pvs, o.uvs, o.ips, o.pagesSum, o.onePageofSession, o.sessionDurationSum, o.sessionCount, o.newUserCount).mkString(",")
  }


  def statSessionSum(r1: SessionSum, clientSession: SessionBase, oldDuration: Long, appendDuration: Long): SessionSum = {

    if (clientSession.pages == 1)
      r1.onePageofSession += 1
    r1.pagesSum += clientSession.pages
    //这个地方比较复杂一点，主要是要去掉补偿的问题
    if (appendDuration == 0) //无需补偿
      r1.sessionDurationSum += clientSession.pageDurationSum
    else {
      r1.sessionDurationSum += appendDuration - oldDuration
    }
    r1.sessionCount += 1
    if (clientSession.newUser == 0)
      r1.newUserCount += 1
    r1
  }


  def sumSessionSum[K <: SessionSum](r1: K, r2: K, doSessionCountSum: Boolean): SessionSum = {
    val sumSession = new SessionSum
    sumSession.pvs = r1.pvs + r2.pvs
    sumSession.uvs = r1.uvs + r2.uvs
    sumSession.ips = r1.ips + r2.ips
    sumSession.onePageofSession = r1.onePageofSession + r2.onePageofSession
    sumSession.pagesSum = r1.pagesSum + r2.pagesSum
    sumSession.newUserCount = r1.newUserCount + r2.newUserCount
    sumSession.sessionDurationSum = r1.sessionDurationSum + r2.sessionDurationSum
    if (doSessionCountSum) {
      sumSession.sessionCount = r1.sessionCount + r2.sessionCount
    } else {
      sumSession.sessionCount = 1
    }
    sumSession
  }


  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr$name"
  }

  def getSessionKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:Detail:$name"
  }

  def getSessionSumKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:SUM:$name"
  }

  def getFinalKey(redisKey: String, key: String): String = {
    val expand_Key = redisKey + ":" + key //展开后的key
    val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
    val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

    final_key
  }

}
