package com.ald.stat.component.stat

import java.util.Date

import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils, HbaseUtiles}
import org.apache.commons.beanutils.BeanUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

trait UV extends StatBase {

  val name = "UV"

  def stat[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))). //去重
      map(record => (statTrait.getBaseKey(record), 1l)).reduceByKey((x, y) => (x + y)) //去掉UID算和
  }

  /**
   * 计算离线的pv
   *
   * @param statTrait
   * @param logRecords
   * @tparam P
   * @tparam K
   * @return
   */
  def statPVOffline[P <: DimensionKey : ClassTag, K <: SubDimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[LogRecord, K, P], logRecords: RDD[LogRecord]): RDD[(P, Long)] = {
    //.filter(record => record.ev == "page")
    val finalRecordRDD = logRecords.map(record => {
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
    finalRecordRDD.map(record => (statTrait.getKey(record))).map(k => (statTrait.getBaseKey(k), 1l)).reduceByKey((x, y) => (x + y))
  }

  /**
   * 旧版广告监测
   * 计算离线的uv
   *
   * @param statTrait
   * @param logRecords
   * @tparam P
   * @tparam K
   * @return
   */
  def statUVOffline[P <: DimensionKey : ClassTag, K <: SubDimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[LogRecord, K, P], logRecords: RDD[LogRecord]): RDD[(P, Long)] = {
    //.filter(record => record.ev == "page")
    val finalRecordRDD = logRecords.map(record => {
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
    finalRecordRDD.map(record => (statTrait.getKey(record))).distinct().map(k => (statTrait.getBaseKey(k), 1l)).reduceByKey((x, y) => (x + y))
  }

  /**
   * 新版广告监测离线
   * UV的计算，
   * 和旧版相同
   *
   * @param statTrait
   * @param logRecords
   * @tparam P
   * @tparam K
   * @return
   */
  def statUVOffline_2[P <: DimensionKey : ClassTag, K <: SubDimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[LogRecord, K, P], logRecords: RDD[LogRecord]): RDD[(P, Long)] = {
    //.filter(record => record.ev == "page")
    val finalRecordRDD = logRecords.map(record => {
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
    finalRecordRDD.map(record => (statTrait.getKey(record))).distinct().map(k => (statTrait.getBaseKey(k), 1l)).reduceByKey((x, y) => (x + y))
  }

  /**
   * 计算离线的uv
   *
   * @param statTrait
   * @param logRecords
   * @tparam P
   * @tparam K
   * @return
   */
  def fullAmountStatUV[P <: DimensionKey : ClassTag, K <: SubDimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[LogRecord, K, P], logRecords: RDD[LogRecord]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct().map(k => (statTrait.getBaseKey(k), 1l)).reduceByKey((x, y) => (x + y))
  }

  /**
   * 补偿计算PV
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @return
   */
  def statPatchPV[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyTrait[C, K], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(K, Long)] = {
    //.filter(record => record.ev == "page")
    logRecords.map(record => (statTrait.getKey(record), 1l)).reduceByKey((x, y) => (x + y)).mapPartitions(par => {
      val arrayBuffer = ArrayBuffer[(K, Long)]()
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) { resource =>
        par.foreach(record => {
          val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
          resource.set(pvKey, record._2.toString) //替换redis中的PV
          resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          arrayBuffer.+=((record._1, record._2))
        })
      }
      arrayBuffer.iterator
    })
  }

  def statPatchSubPV[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyTrait[C, K], logRecords: RDD[C], redisPrefix: Broadcast[String], patchHour: String, prefix: String): RDD[(K, Long)] = {
    //.filter(record => record.ev == "page")
    logRecords.map(record => (statTrait.getKey(record), 1l)).reduceByKey((x, y) => (x + y)).mapPartitions(par => {
      val arrayBuffer = ArrayBuffer[(K, Long)]()
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) { resource =>
        par.foreach(record => {
          //手动拼出hourlyKey
          var count = 0
          val keySplited = record._1.key.split(":")
          var keyString = ""
          for (str <- keySplited) {
            if (count == 1) {
              keyString += str + " "
              keyString += patchHour + " "
            } else {
              keyString += str + " "
            }
            count += 1
          }
          val markedHourKey = keyString.trim.replaceAll(" ", ":")
          //          val markedHourKey = record._1.key + ":" + patchHour
          val pvMarkedHourKey = getMarkKey(dateStr, baseRedisKey, markedHourKey, "pv") + ":" + prefix
          val pvHourKey = pvMarkedHourKey.replace("Daily", "Hour")
          val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
          var pvSum = 0l
          if (resource.exists(pvHourKey) == true) {
            val pvSub = record._2 - resource.get(pvHourKey).toInt //减去当前小时redis中已经计数的pv
            if (resource.exists(pvKey) == true) {
              pvSum = resource.get(pvKey).toInt + pvSub
              resource.set(pvKey, pvSum.toString) //替换redis中的PV
              resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              arrayBuffer.+=((record._1, pvSum))
            } else {
              resource.set(pvKey, pvSub.toString) //替换redis中的PV
              resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              arrayBuffer.+=((record._1, pvSub))
            }
          } else {
            if (resource.exists(pvKey) == true) {
              pvSum = resource.get(pvKey).toInt + record._2
              resource.set(pvKey, pvSum.toString) //替换redis中的PV
              resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              arrayBuffer.+=((record._1, pvSum))
            } else {
              resource.set(pvKey, record._2.toString) //替换redis中的PV
              resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              arrayBuffer.+=((record._1, record._2))
            }
          }
        })
      }
      arrayBuffer.iterator.foreach(x => {
        println(x)
      })
      arrayBuffer.iterator
    })
  }


  /**
   * 补偿计算UV
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def statPatchUV[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct()
      .mapPartitions(par => {
        val csts = ArrayBuffer[(P, Long)]()
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        use(redisCache.getResource) {
          resource => {
            par.foreach(record => {
              var uidKey: String = dateStr
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
              val expand_Key = uidKey + ":" + key //展开后的key
              val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
              val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
              if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                resource.set(final_key, "1") //不存在，则添加，并赋值为1
                resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
              csts.+=((statTrait.getBaseKey(record), 1l))
            })
          }
        }
        csts.iterator
      }).reduceByKey((x, y) => (x + y)).mapPartitions(par => {
      val arrayBuffer = ArrayBuffer[(P, Long)]()
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) { resource =>
        par.foreach(record => {
          val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
          resource.set(uvKey, record._2.toString) //替换redis中的UV
          resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          arrayBuffer.+=((record._1, record._2))
        })
      }
      arrayBuffer.iterator
    })
  }

  /**
   * 补偿计算UV  天 累加计算，不做替换
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def statPatchUVNew[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct()
      .mapPartitions(par => {
        val csts = ArrayBuffer[(P, Long)]()
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        use(redisCache.getResource) {
          resource => {
            par.foreach(record => {
              var uidKey: String = dateStr
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
              val expand_Key = uidKey + ":" + key //展开后的key
              val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
              val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
              if (resource.exists(final_key) == false) {
                resource.set(final_key, "1") //不存在，则添加，并赋值为1
                resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                csts.+=((statTrait.getBaseKey(record), 1l))
              }
            })
          }
        }
        csts.iterator
      }).reduceByKey((x, y) => (x + y)).mapPartitions(par => {
      val arrayBuffer = ArrayBuffer[(P, Long)]()
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) { resource =>
        par.foreach(record => {
          val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
          val uv = resource.incrBy(uvKey, record._2)
          resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          arrayBuffer.+=((record._1, uv))
        })
      }
      arrayBuffer.iterator
    })
  }

  /**
   * Cache中需要存入当日UID，如果发现有相同的UID应该要算0，所以原先方法是有问题的
   *
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @tparam C
   * @tparam K
   * @tparam P
   */
  def statIncreaseCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))). //当时所有的uid已经去掉重复的
      mapPartitions( //现在判断是否要计数
        par => {
          val resource = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache].getResource
          val uidKey = getDetailKey(dateStr, baseRedisKey)
          val csts = ArrayBuffer[(P, Long)]()
          import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
          val cacheSet: Set[Int] = mutable.HashSet()

          par.foreach(record => {
            val key = statTrait.getKeyofDay(record).toString //当日只有一个
            val baseHashKey = statTrait.getBaseKey(record)
            val unionKey = (uidKey + key).hashCode
            val uvKey = uidKey + ":" + "uv"
            var uv = 0l
            //获取原uv
            val oUvStr = resource.get(uvKey)
            if (StringUtils.isNotBlank(oUvStr)) {
              try {
                uv = oUvStr.toLong
              } catch {
                case e: Throwable => {
                  uv = 0l
                  e.printStackTrace()
                }
              }
            }
            //本地set直接判断
            if (!cacheSet.contains(unionKey)) {
              if (resource.sadd(uidKey, key) == 1) {
                cacheSet.add(unionKey)
                uv = resource.incr(uvKey)
              } else {
                cacheSet.add(unionKey)
              }
            }
            resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
            csts.+=((baseHashKey, uv))
          })
          //设置失效时间
          resource.expireAt(uidKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          try {
            if (resource != null)
              resource.close()
          }
          catch {
            case t: Throwable => t.printStackTrace()
          }
          csts.iterator
        }).reduceByKey((x, y) => (Math.max(x, y)))
  }

  def getDetailKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:UID:$name"
  }

  /**
   * Cache中需要存入当日UID，如果发现有相同的UID应该要算0，所以原先方法是有问题的
   *
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @tparam C
   * @tparam K
   * @tparam P (k,pv,uv)
   */
  def statIncreaseCacheWithPV_expire[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, (Long, Long))] = {

    logRecords.filter(record => record.pp != "useless_page").map(record => (statTrait.getKey(record))).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              var tuple = (1l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
              if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                val expand_Key = uidKey + ":" + key //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                  resource.set(final_key, "1") //不存在，则添加，并赋值为1
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                  tuple = (1, 1)
                } else {
                  cacheSet.add(unionKey)
                  tuple = (1, 0)
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
                arrayBuffer.+=((record._1, (resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  def statIncreaseCacheWithPV[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, (Long, Long))] = {

    logRecords.filter(record => record.pp != "useless_page").map(record => (statTrait.getKey(record), 1)).reduceByKey(_ + _).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record._1).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record._1) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              val pvs = record._2.toLong
              var tuple = (pvs, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
              if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                val expand_Key = uidKey + ":" + key //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                if (resource.exists(final_key) == false) { //不存在返回1，存在返回0
                  resource.set(final_key, "1") //不存在，则添加，并赋值为1
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                  tuple = (pvs, 1)
                } else {
                  cacheSet.add(unionKey)
                  tuple = (pvs, 0)
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
                arrayBuffer.+=((record._1, (resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 新版广告监测实时，
   * 计算外链部分的pv和uv，
   * 返回与redis聚合后的pvAndUv 和 当前批次的BatchpvAndUv，
   * 和旧版实时相同
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def statIncreaseCacheWithPVLink_2[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, ((Long, Long), (Long, Long)))] = {

    logRecords.filter(record => record.pp != "useless_page")
      .map(record => (statTrait.getKey(record), 1))
      .reduceByKey(_ + _)
      .mapPartitions(par => {
        val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
        val csts = ArrayBuffer[(P, (Long, Long))]()
        try {
          use(redisCache.getResource) {
            resource => {
              import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
              val cacheSet: Set[Int] = Set()
              var uidKey: String = dateStr
              par.foreach(record => {
                uidKey = getDetailKey(dateStr, baseRedisKey)
                val key = statTrait.getKeyofDay(record._1).toString //获取subDimensionKey的hash值
                val baseKey = statTrait.getBaseKey(record._1) //获取dimensionKey
                val unionKey = (uidKey + key).hashCode
                val pvs = record._2.toLong
                var tuple = (pvs, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
                if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                  val expand_Key = uidKey + ":" + key //展开后的key
                  val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                  val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                  if (resource.exists(final_key) == false) { //不存在返回uv=1，存在返回uv=0
                    resource.set(final_key, "1") //不存在，则添加，并给redis的这个key赋值为1
                    resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                    cacheSet.add(unionKey)
                    tuple = (pvs, 1)
                  } else {
                    cacheSet.add(unionKey)
                    tuple = (pvs, 0)
                  }
                }
                csts.+=((baseKey, tuple))
              })
            }
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }
        csts.iterator
      }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, ((Long, Long), (Long, Long)))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
                arrayBuffer.+=((record._1, ((resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2)), (record._2._1, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 旧版广告监测实时
   * 计算外链部分的pv和uv
   * 返回当前批次的pvAndUv和与redis聚合后的pvAndUv
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def statIncreaseCacheWithPVLink[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, ((Long, Long), (Long, Long)))] = {

    logRecords.filter(record => record.pp != "useless_page").map(record => (statTrait.getKey(record), 1)).reduceByKey(_ + _).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record._1).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record._1) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              val pvs = record._2.toLong
              var tuple = (pvs, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
              if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                val expand_Key = uidKey + ":" + key //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                  resource.set(final_key, "1") //不存在，则添加，并赋值为1
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                  tuple = (pvs, 1)
                } else {
                  cacheSet.add(unionKey)
                  tuple = (pvs, 0)
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, ((Long, Long), (Long, Long)))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
                arrayBuffer.+=((record._1, ((resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2)), (record._2._1, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 旧版广告监测实时
   * 为了join，将返回值的key转换成String类型
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def statIncreaseCacheWithPVOnline[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(String, (Long, Long))] = {

    logRecords.filter(record => record.pp != "useless_page").map(record => (statTrait.getKey(record), 1)).reduceByKey(_ + _).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record._1).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record._1) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              val pvs = record._2.toLong
              var tuple = (pvs, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
              if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                val expand_Key = uidKey + ":" + key //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                  resource.set(final_key, "1") //不存在，则添加，并赋值为1
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                  tuple = (pvs, 1)
                } else {
                  cacheSet.add(unionKey)
                  tuple = (pvs, 0)
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(String, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
                arrayBuffer.+=((record._1.toString, (resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 新版广告监测实时
   * 为了join，将返回值的key转换成String类型
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def statIncreaseCacheWithPVOnline_2[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(String, (Long, Long))] = {

    logRecords.filter(record => record.pp != "useless_page").map(record => (statTrait.getKey(record), 1)).reduceByKey(_ + _).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record._1).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record._1) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              val pvs = record._2.toLong
              var tuple = (pvs, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
              if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                val expand_Key = uidKey + ":" + key //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                  resource.set(final_key, "1") //不存在，则添加，并赋值为1
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                  tuple = (pvs, 1)
                } else {
                  cacheSet.add(unionKey)
                  tuple = (pvs, 0)
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(String, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
                arrayBuffer.+=((record._1.toString, (resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  //计算扩张维度的新增授权用户累加,用于离线计算
  def reduceAuth(resultArray: RDD[(DimensionKey, (Long, Long))], ssc: SparkSession, hour: String, wsr_query_ald_link_key: String,
                 wsr_query_ald_media_id: String, ag_ald_position_id: String): RDD[(String, Long)] = {
    resultArray.map(i => {
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
      (baseKey, i._2._2)
    }).reduceByKey(_ + _)
  }

  /**
   * 用于根据不同维度聚合新授权的数量
   *
   * @param authUser 最细力度计算的新授权数
   * @param hour
   * @param wsr_query_ald_link_key
   * @param wsr_query_ald_media_id
   * @param ag_ald_position_id
   * @return
   */
  def reduceAuthIndependentLink(authUser: RDD[(DimensionKey, Long)], hour: String, wsr_query_ald_link_key: String,
                                wsr_query_ald_media_id: String, ag_ald_position_id: String): RDD[(String, Long)] = {
    authUser.map(i => {
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
      (baseKey, i._2)
    }).reduceByKey(_ + _)
  }

  /**
   * 用于旧版广告检测实时计算,
   * 计算扩张维度的新增授权用户累加
   * @param resultArray
   * @param ssc
   * @param hour
   * @param wsr_query_ald_link_key
   * @param wsr_query_ald_media_id
   * @param ag_ald_position_id
   * @param redisPrefix
   * @param dateStr
   * @param baseRedisKey
   * @return
   */
  def reduceAuthOnline(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], ssc: StreamingContext, hour: String, wsr_query_ald_link_key: String,
                       wsr_query_ald_media_id: String, ag_ald_position_id: String, redisPrefix: Broadcast[String], dateStr: String, baseRedisKey: String): RDD[(String, Long)] = {
    val arrayBuffer = ArrayBuffer[(String, Long)]()
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
      arrayBuffer.+=((baseKey, i._2._2))
    }
    val authRDD = ssc.sparkContext.parallelize(arrayBuffer)
    val finalRDD = authRDD.reduceByKey(_ + _).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(String, Long)]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(record => {
              val uidKey = getDetailKey(dateStr, baseRedisKey)
              val expand_Key_hash = HashUtils.getHash(record._1).toString //对整个key取hash值
              val final_key = expand_Key_hash + ":" + uidKey //将完整的key，取hash值并前置，得到最终的存入redis的key
              val authKey = getMarkKey(dateStr, baseRedisKey, final_key, "uv")
              arrayBuffer.+=((record._1, resource.incrBy(authKey, record._2)))
              //设置失效时间
              resource.expireAt(authKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
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
   * 用于新版广告检测实时计算,
   * 计算扩张维度的新增授权用户累加
   * @param resultArray
   * @param ssc
   * @param hour
   * @param wsr_query_ald_link_key
   * @param wsr_query_ald_media_id
   * @param ag_ald_position_id
   * @param redisPrefix
   * @param dateStr
   * @param baseRedisKey
   * @return
   */
  def reduceAuthOnline_2(resultArray: Array[(DimensionKey, (((Long, Long), SessionSum), Long))], ssc: StreamingContext, hour: String, wsr_query_ald_link_key: String,
                       wsr_query_ald_media_id: String, ag_ald_position_id: String, redisPrefix: Broadcast[String], dateStr: String, baseRedisKey: String): RDD[(String, Long)] = {
    val arrayBuffer = ArrayBuffer[(String, Long)]()
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
      arrayBuffer.+=((baseKey, i._2._2))
    }
    val authRDD = ssc.sparkContext.parallelize(arrayBuffer)
    val finalRDD = authRDD.reduceByKey(_ + _).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(String, Long)]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(record => {
              val uidKey = getDetailKey(dateStr, baseRedisKey)
              val expand_Key_hash = HashUtils.getHash(record._1).toString //对整个key取hash值
              val final_key = expand_Key_hash + ":" + uidKey //将完整的key，取hash值并前置，得到最终的存入redis的key
              val authKey = getMarkKey(dateStr, baseRedisKey, final_key, "uv")
              arrayBuffer.+=((record._1, resource.incrBy(authKey, record._2)))
              //设置失效时间
              resource.expireAt(authKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
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
   * 新增授权用户
   * 通过ifo和img计算新增授权用户
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def newAuthUserWithUV[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, (Long, Long))] = {

    logRecords.filter(record => record.op == "useless_openid").map(record => (statTrait.getKey(record))).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              val subDimensionKey_array = record.toString.split(":")
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              var tuple = (1l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1

              if (subDimensionKey_array(subDimensionKey_array.size - 2) == "true" && subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") { //如果ifo=true 并且 img!=useless_img,则为真正需要计数的新增授权数据
                if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                  val expand_Key = uidKey + ":" + key //展开后的key
                  val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                  val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                  if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                    resource.set(final_key, "1") //不存在，则添加，并赋值为1
                    resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                    cacheSet.add(unionKey)
                    tuple = (1, 1)
                  } else {
                    cacheSet.add(unionKey)
                    tuple = (1, 0)
                  }
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pvxx")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "auth")
                arrayBuffer.+=((record._1, (resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 旧版广告监测实时
   * 新增授权用户
   * 通过openid计算新增授权用户
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def newAuthUserWithOpenId[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, ((Long, Long), (Long, Long)))] = {

    val newAuthUserFromOpenID = logRecords.filter(record => record.op != "useless_openid").map(record => (statTrait.getKey(record))).mapPartitions(par => {
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        var uidKey: String = dateStr
        par.foreach(record => {
          var tuple = (1l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
          val subDimensionKey_array = record.toString.split(":")
          //openId + ak 生成rowKey，从Hbase获取rowkey下的列值
          if (subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") {
            val openId = subDimensionKey_array(subDimensionKey_array.size - 3)
            val app_key = subDimensionKey_array(0)
            val rowKey = openId + ":" + app_key
            val hbaseColumn = "online"
            val hbaseAuthColumn = "onlineStatus"
            val currentTime = (new Date()).getTime
            val result = HbaseUtiles.getDataFromHTable(rowKey, currentTime, hbaseColumn)
            if (result._1 == "notStored") {
              tuple = (1, 1)
              HbaseUtiles.getDataFromHTable(rowKey, currentTime, hbaseAuthColumn)
            } else {
              val resultAuth = HbaseUtiles.getDataFromHTable(rowKey, currentTime, hbaseAuthColumn)
              if (resultAuth._1 == "notStored") {
                tuple = (1, 1)
              } else {
                tuple = (1, 0)
              }
            }
          }
          val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
          csts.+=((baseKey, tuple))
        })
      } finally {
        println("connect to hbase failed ------------------mine")
      }
      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, ((Long, Long), (Long, Long)))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pvxx")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "auth")
                arrayBuffer.+=((record._1, ((resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2)), (record._2._1, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })

    val newAuthUserFromIfo = logRecords.filter(record => record.op == "useless_openid").map(record => (statTrait.getKey(record))).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              val subDimensionKey_array = record.toString.split(":")
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              var tuple = (1l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1

              if (subDimensionKey_array(subDimensionKey_array.size - 2) == "true" && subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") { //如果ifo=true 并且 img!=useless_img,则为真正需要计数的新增授权数据
                if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                  val expand_Key = uidKey + ":" + key //展开后的key
                  val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                  val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                  if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                    resource.set(final_key, "1") //不存在，则添加，并赋值为1
                    resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                    cacheSet.add(unionKey)
                    tuple = (1, 1)
                  } else {
                    cacheSet.add(unionKey)
                    tuple = (1, 0)
                  }
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, ((Long, Long), (Long, Long)))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pvxx")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "auth")
                arrayBuffer.+=((record._1, ((resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2)), (record._2._1, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
    newAuthUserFromOpenID.union(newAuthUserFromIfo)
  }

  /**
   * 新版广告监测实时，
   * 新增授权用户，
   * 通过openid计算新增授权用户。
   * 分别返回全量pv uv，增量pv uv
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def  newAuthUserWithOpenId_2[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String], currentTime: Long,
   newUserTableName: String, newUserHbaseColumn: String, authUserTableName: String, linkAuthColumn: String): RDD[(P, ((Long, Long), (Long, Long)))] = {

    val newAuthUserFromOpenID = logRecords.filter(record => record.op != "useless_openid").map(record => (statTrait.getKey(record))).mapPartitions(par => {
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        var uidKey: String = dateStr
        par.foreach(record => {
          var tuple = (1l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
          // subDimensionKey_array长度为8：ak，dayHour._1，dayHour._2，wsr_query_ald_link_key，wsr_query_ald_media_id，wsr_query_ald_position_id，op，img
          val subDimensionKey_array = record.toString.split(":")
          //openId + ak 生成rowKey，从Hbase获取rowkey下的列值
          if (subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") {
            val openId = subDimensionKey_array(subDimensionKey_array.size - 2)
            val app_key = subDimensionKey_array(0)
            val rowKey = openId + ":" + app_key
//            val hbaseColumn = "online"
//            val hbaseAuthColumn = "onlineStatus"
//            val currentTime = (new Date()).getTime
            val result = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, newUserHbaseColumn, newUserTableName)
            if (result._1 == "notStored") {
              tuple = (1, 1)
              HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, linkAuthColumn, authUserTableName)
            } else {
              val resultAuth = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, linkAuthColumn, authUserTableName)
              if (resultAuth._1 == "notStored") {
                tuple = (1, 1)
              } else {
                tuple = (1, 0)
              }
            }
          }
          val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
          csts.+=((baseKey, tuple))
        })
      } finally {
        println("connect to hbase failed ------------------mine")
      }
      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, ((Long, Long), (Long, Long)))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pvxx")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "auth")
                arrayBuffer.+=((record._1, ((resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2)), (record._2._1, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })

    newAuthUserFromOpenID
  }


  /**
   * 旧版广告检测离线计算
   * 新增授权用户
   * 通过openid计算新增授权用户
   *
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def newAuthUserWithOpenIdOffline[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], currentTime: Long, hbaseColumn: String, day_wz: String): RDD[(P, (Long, Long))] = { // 可以在任何地方引入 可变集合
    //    val cacheSet: Set[Int] = Set()
    val newAuthUserFromOpenID = logRecords.filter(record => record.op != "useless_openid").map(record => (statTrait.getKey(record))).distinct().mapPartitions(par => {
      val csts = ArrayBuffer[(P, (Long, Long))]()
      var uidKey: String = dateStr
      par.foreach(record => {
        val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
        val unionKey = (uidKey + key).hashCode
        var tuple = (0l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
        val subDimensionKey_array = record.toString.split(":")
        //openId + ak 生成rowKey，从Hbase获取rowkey下的列值
        val hbaseAuthColumn = "offlineStatus"
        if (subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") {
          val openId = subDimensionKey_array(subDimensionKey_array.size - 3)
          val app_key = subDimensionKey_array(0)
          val rowKey = openId + ":" + app_key
          val resultAuth = HbaseUtiles.getDataFromHTable(rowKey, currentTime, hbaseAuthColumn)
          if (resultAuth._1 == "notStored") {
            tuple = (0, 1)
          } else if (resultAuth._1 == currentTime.toString) {
            tuple = (0, 0)
          } else if (resultAuth._1.toLong > ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)) {
            HbaseUtiles.insertTable(rowKey, hbaseColumn, currentTime)
            tuple = (0, 1)
          }
        }
        //              if (!cacheSet.contains(unionKey)) {
        //                cacheSet.add(unionKey)
        //              }
        val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
        csts.+=((baseKey, tuple))
      })
      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      par.foreach(
        record => {
          arrayBuffer.+=((record._1, (record._2._1, record._2._2)))
        }
      )
      //全量RDD返回
      arrayBuffer.iterator
    })
    val cacheImg = mutable.Set[String]()
    val newAuthUserFromIfo = logRecords.filter(record => record.op == "useless_openid").map(record => (statTrait.getKey(record), 1l)).reduceByKey(_ + _).map(record => {
      //      val csts = ArrayBuffer[(P, (Long, Long))]()
      var uidKey: String = dateStr
      //            par.foreach(record => {
      val subDimensionKey_array = record._1.toString.split(":")
      //              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
      val baseKey = statTrait.getBaseKey(record._1) //获取dimensionKey
      //              val unionKey = (uidKey + key).hashCode
      val unionKey = subDimensionKey_array(subDimensionKey_array.size - 3)
      var tuple = (0l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1

      if (subDimensionKey_array(subDimensionKey_array.size - 2) == "true" && subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") { //如果ifo=true 并且 img!=useless_img,则为真正需要计数的新增授权数据
        if (!cacheImg.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
          tuple = (0l, 1l)
          cacheImg.add(unionKey)
        }
      }
      (baseKey, tuple)
      //            })
      //      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      par.foreach(
        record => {
          arrayBuffer.+=((record._1, (record._2._1, record._2._2)))
        }
      )
      //全量RDD返回
      arrayBuffer.iterator
    })
    val a = newAuthUserFromIfo.fullOuterJoin(newAuthUserFromOpenID).map(r => {
      val authUu = r._2._1.getOrElse(0l, 0l)
      val authOpenId = r._2._2.getOrElse(0l, 0l)
      if (authOpenId != (0l, 0l)) {
        (r._1, authOpenId)
      } else {
        (r._1, authUu)
      }
    })
    a
  }

  /**
   * 新版广告监测离线
   * 新增授权用户
   * 通过openid计算新增授权用户
   * 用于独立的广告监测离线计算
   *
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def newAuthUserForIndependentLink[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], currentTime: Long, hbaseColumn: String, day_wz: String,
   authUserTableName: String): RDD[(P, Long)] = { // 可以在任何地方引入 可变集合
    //    val cacheSet: Set[Int] = Set()
    logRecords.filter(record => record.op != "useless_openid").map(record => (statTrait.getKey(record))).distinct().mapPartitions(par => {
      val csts = ArrayBuffer[(P, Long)]()
      var uidKey: String = dateStr
      par.foreach(record => {
        val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
        val unionKey = (uidKey + key).hashCode
        var tuple = 0l //创建一个元组（pv,uv）,pv的默认值就是1
        val subDimensionKey_array = record.toString.split(":")
        //openId + ak 生成rowKey，从Hbase获取rowkey下的列值
        if (subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") {
          val openId = subDimensionKey_array(subDimensionKey_array.size - 2)
          val app_key = subDimensionKey_array(0)
          val rowKey = openId + ":" + app_key
          val resultAuth = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, hbaseColumn, authUserTableName)
          if (resultAuth._1 == "notStored") {
            tuple = 1
          } else if (resultAuth._1 == currentTime.toString) {
            tuple = 0
          } else if (resultAuth._1.toLong < ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)) {
            tuple = 0
          } else {
            tuple = 1
          }
        }
        val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
        csts.+=((baseKey, tuple))
      })
      csts.iterator
      //规约增量
    }).reduceByKey(_ + _).mapPartitions(par => {
      //获取全量
      val arrayBuffer = ArrayBuffer[(P, Long)]()
      par.foreach(
        record => {
          arrayBuffer.+=((record._1, record._2))
        }
      )
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 新版广告监测实时
   * 新增授权用户
   * 通过openid计算新增授权用户
   * 用于独立的广告监测离线计算
   *
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def newAuthUserForNewAdMonitor[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], currentTime: Long, hbaseColumn: String,
   authUserTableName: String): RDD[(P, Long)] = { // 可以在任何地方引入 可变集合
    //    val cacheSet: Set[Int] = Set()
    logRecords.filter(record => record.op != "useless_openid").map(record => (statTrait.getKey(record))).distinct().mapPartitions(par => {
      val csts = ArrayBuffer[(P, Long)]()
      var uidKey: String = dateStr
      par.foreach(record => {
        val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
        val unionKey = (uidKey + key).hashCode
        var tuple = 0l //创建一个元组（pv,uv）,pv的默认值就是1
        val subDimensionKey_array = record.toString.split(":")
        //openId + ak 生成rowKey，从Hbase获取rowkey下的列值
        if (subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") {
          val openId = subDimensionKey_array(subDimensionKey_array.size - 2)
          val app_key = subDimensionKey_array(0)
          val rowKey = openId + ":" + app_key
          val resultAuth = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, hbaseColumn, authUserTableName)
          if (resultAuth._1 == "notStored") {
            tuple = 1
          } else {
            tuple = 0
          }
        }
        val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
        csts.+=((baseKey, tuple))
      })
      csts.iterator
      //规约增量
    }).reduceByKey(_ + _).mapPartitions(par => {
      //获取全量
      val arrayBuffer = ArrayBuffer[(P, Long)]()
      par.foreach(
        record => {
          arrayBuffer.+=((record._1, record._2))
        }
      )
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 补偿新增授权用户
   *
   * @param baseRedisKey
   * @param dateStr
   * @param statTrait
   * @param logRecords
   * @param redisPrefix
   * @tparam C
   * @tparam K
   * @tparam P
   * @return
   */
  def statPatchAuth[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {

    logRecords.map(record => (statTrait.getKey(record))).mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val csts = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource => {
            import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
            val cacheSet: Set[Int] = Set()
            var uidKey: String = dateStr
            par.foreach(record => {
              val subDimensionKey_array = record.toString.split(":")
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
              val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
              val unionKey = (uidKey + key).hashCode
              var tuple = (1l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1

              if (subDimensionKey_array(subDimensionKey_array.size - 2) == "true" && subDimensionKey_array(subDimensionKey_array.size - 1) != "useless_img") { //如果ifo=true 并且 img!=useless_img,则为真正需要计数的新增授权数据
                if (!cacheSet.contains(unionKey)) { //本地set直接判断，用于当前批次内的快速去重
                  val expand_Key = uidKey + ":" + key //展开后的key
                  val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                  val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key
                  if (resource.exists(final_key) == false) { //存在返回1，不存在返回0
                    resource.set(final_key, "1") //不存在，则添加，并赋值为1
                    resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                    cacheSet.add(unionKey)
                    tuple = (1, 1)
                  } else {
                    cacheSet.add(unionKey)
                    tuple = (1, 0)
                  }
                }
              }
              csts.+=((baseKey, tuple))
            })
          }
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      csts.iterator
      //规约增量
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, Long)]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pvxx")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "auth")
                arrayBuffer.+=((record._1, resource.incrBy(uvKey, record._2._2)))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
   * 获取 指定 key 下 pv uv 数据
   *
   * @param keys         DimensionKey keys
   * @param dateStr      date
   * @param baseRedisKey redis key prefix
   * @param redisPrefix  模块redis prefix
   * @tparam P
   * @return
   */
  def getCachedKeysPvUv[P <: DimensionKey : ClassTag : Ordering](keys: RDD[P], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): RDD[(P, (Long, Long))] = {
    keys.mapPartitions(par => {
      val redisCache = CacheRedisFactory.getInstances(redisPrefix.value).asInstanceOf[AbstractRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pv = resource.get(getMarkKey(dateStr, baseRedisKey, record.key, "pv"))
                val uv = resource.get(getMarkKey(dateStr, baseRedisKey, record.key, "uv"))
                var pvVal = 0l
                var uvVal = 0l
                if (StringUtils.isNotBlank(pv)) {
                  pvVal = pv.toLong
                }
                if (StringUtils.isNotBlank(uv)) {
                  uvVal = uv.toLong
                }
                arrayBuffer.+=((record, (pvVal, uvVal))
                )
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }

  /**
   *
   * @param dateStr
   * @param baseKey
   * @param key
   * @param mark pv|uv
   * @return
   */
  def getMarkKey(dateStr: String, baseKey: String, key: String, mark: String): String = {
    s"$baseKey:$dateStr:$name:$key:$mark"
  }

  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:$name"
  }

}
