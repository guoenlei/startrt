package com.ald.stat.appletJob

import java.util
import java.util.Date

import com.ald.stat.cache.{AbstractRedisCache, BaseRedisCache, CacheRedisFactory}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils.HbaseUtiles.insertTableWithName
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable.ArrayBuffer

/**
 *
 */
class AbstractBaseJob {


  /**
   * 对old sdk 处理new user
   *
   * @param stream
   * @param baseRedisKey
   */
  def handleNewUserForOldSDK(stream: InputDStream[ConsumerRecord[String, String]], baseRedisKey: String, prefix: String): Unit = {
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
        try {
          use(redisCache.getResource) {
            resource =>
              par.foreach(line => {
                val logRecord = LogRecord.line2Bean(line.value())
                if (logRecord != null && logRecord.ifo == "true"
                  && logRecord.v < "7.0.0") {
                  val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
                  val key = s"$baseRedisKey:$d:newUser"
                  resource.hset(key, logRecord.at.trim, "true")
                  resource.expireAt(key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                }
              })
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }
      })
    })
  }


  /**
   * 标记新用户并维护最大的offset
   *
   * @param stream
   * @param group
   * @param baseRedisKey
   * @param prefix
   * @param offset_prefix
   */
  def handleNewUserAndLatestOffset(stream: InputDStream[ConsumerRecord[String, String]], group: String,
                                   baseRedisKey: String, prefix: String, offset_prefix: String): Unit = {
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        var latest_record: ConsumerRecord[String, String] = null
        val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
        try {
          use(redisCache.getResource) {
            resource =>
              par.foreach(line => {
                val logRecord = LogRecord.line2Bean(line.value())
                if (logRecord != null && logRecord.at != null &&
                  logRecord.et != null && logRecord.ifo == "true") { //&& logRecord.v < "7.0.0"
                  val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
                  val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim //展开后的key
                  val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                  val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

                  if (resource.exists(final_key) == false) { //如果不存在则添加
                    resource.set(final_key, "true")
                    resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  }
                }
                latest_record = line //遍历，赋值，最终得到最后一条记录
              })
          }
        } finally {
          if (redisCache != null) redisCache.close()
        }
        //维护最大的offset
        if (latest_record != null) {
          val dateStr = ComputeTimeUtils.getDateStr(new Date())
          val offsetRedisCache = CacheRedisFactory.getInstances(offset_prefix).asInstanceOf[AbstractRedisCache]
          //          val resource_offset = offsetRedisCache.getResource
          use(offsetRedisCache.getResource) { resource_offset =>
            RddUtils.checkAndSaveLatestOffset(dateStr, latest_record.topic(), group, latest_record, resource_offset)
          }
          //          try{

          //          }finally {
          //            if (resource_offset != null) resource_offset.close()
          //            if (offsetRedisCache != null) offsetRedisCache.close()
          //          }
        }

      })
    })
  }


  /**
   * 标记新用户
   *
   * @param rdd
   * @param group
   * @param baseRedisKey
   * @param prefix
   * @param offset_prefix
   */
  def handleNewUser(rdd: RDD[ConsumerRecord[String, String]], group: String,
                    baseRedisKey: String, prefix: String, offset_prefix: String): Unit = {
    rdd.foreachPartition(par => {
      val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null && logRecord.at != null &&
                logRecord.et != null && logRecord.ifo == "true") { //&& logRecord.v < "7.0.0"
                val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
                val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

                if (resource.exists(final_key) == false) { //如果不存在则添加
                  resource.set(final_key, "true")
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                }
              }
            })
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //维护最大的offset
      //        if(latest_record != null){
      //          val dateStr = ComputeTimeUtils.getDateStr(new Date())
      //          val offsetRedisCache = CacheRedisFactory.getInstances(offset_prefix).asInstanceOf[AbstractRedisCache]
      //          //          val resource_offset = offsetRedisCache.getResource
      //          use(offsetRedisCache.getResource) { resource_offset =>
      //            RddUtils.checkAndSaveLatestOffset(dateStr, latest_record.topic(), group, latest_record, resource_offset)
      //          }
      //          //          try{
      //
      //          //          }finally {
      //          //            if (resource_offset != null) resource_offset.close()
      //          //            if (offsetRedisCache != null) offsetRedisCache.close()
      //          //          }
      //        }

    })
    //    })
  }


  /**
   * 对old sdk 处理new user
   *
   * rdd
   *
   * @param baseRedisKey
   */
  def handleNewUserForOldSDK(rdd: RDD[ConsumerRecord[String, String]], baseRedisKey: String, prefix: String): Unit = {
    import scala.collection.mutable.Set
    val cacheSet: Set[Int] = Set()
    rdd.foreachPartition(par => {
      val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) {
        resource =>
          par.foreach(line => {
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              logRecord.at != null &&
              logRecord.et != null &&
              logRecord.ifo == "true" &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.uu) &&
              logRecord.v != null
              && SDKVersionUtils.isOldSDk(logRecord.v)) {
              val unionKey = (logRecord.ak + logRecord.uu).hashCode
              val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
              if (!cacheSet.contains(unionKey)) {
                val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

                if (resource.exists(final_key) == false) {
                  //如果不存在则添加
                  resource.set(final_key, "true")
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                }
                cacheSet.add(unionKey)
              }
            }
          })
      }
    })
  }

  /**
   * 独立的广告监测的新用户标记
   *
   * rdd
   *
   */
  def handleNewUserForNewLinkOffline(stream_rdd: RDD[LogRecord], newUserHbaseColumn: String, newUserTableName: String, day_wz: String, currentTime: Long): RDD[LogRecord] = {
    import scala.collection.mutable.Set
    val linkCacheSet: Set[Int] = Set()
    val linkRDD = stream_rdd.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.op) &&
          StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
          logRecord.wsr_query_ald_link_key != "null" &&
          logRecord.op != "useless_openid"
        ) {
          val rowKey = logRecord.op.reverse + ":" + logRecord.ak
          val unionKey = (rowKey).hashCode
          if (!linkCacheSet.contains(unionKey)) {
            val result = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, newUserHbaseColumn, newUserTableName)
            if (result._1 == "notStored") {
              logRecord.ifo = "true"
            } else if (result._1 == currentTime.toString) {
              logRecord.ifo = "false"
            } else if (result._1.toLong < ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)) {
              logRecord.ifo = "false"
            } else {
              logRecord.ifo = "true"
              insertTableWithName(rowKey, newUserHbaseColumn, currentTime, newUserTableName)
            }
            linkCacheSet.add(unionKey)
          } else {
            logRecord.ifo = "false"
          }
          recordsRdd += logRecord
        }
      })
      recordsRdd.iterator
    })

    val cacheSet: Set[Int] = Set()
    val amountRDD = stream_rdd.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.op) &&
          logRecord.op != "useless_openid"
        ) {
          if (logRecord.wsr_query_ald_link_key == "null" || StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key)) {
            val rowKey = logRecord.op.reverse + ":" + logRecord.ak
            val unionKey = (rowKey).hashCode
            if (!cacheSet.contains(unionKey)) {
              val result = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, newUserHbaseColumn, newUserTableName)
              if (result._1 == "notStored") {
                logRecord.ifo = "true"
              } else if (result._1 == currentTime.toString) {
                logRecord.ifo = "false"
              } else if (result._1.toLong < ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)) {
                logRecord.ifo = "false"
              } else {
                logRecord.ifo = "true"
                insertTableWithName(rowKey, newUserHbaseColumn, currentTime, newUserTableName)
              }
              cacheSet.add(unionKey)
            } else {
              logRecord.ifo = "false"
            }
            recordsRdd += logRecord
          }
        }
      })
      recordsRdd.iterator
    })
    linkRDD.union(amountRDD)
  }

  /**
   * 独立的新版广告监测V2.0的新用户标记
   * @param logRecordRdd link或total的RDD
   * @param currentTime
   * @param newUserHbaseColumn
   * @param newUserTableName
   * @return
   */
  def handleNewUserForNewAdMonitorOnline(logRecordRdd: RDD[LogRecord], currentTime: Long, newUserHbaseColumn: String, newUserTableName: String) = {
    import scala.collection.mutable.Set
    val linkCacheSet: Set[Int] = Set()
    val linkRDD = logRecordRdd.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.op) &&
          StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
          logRecord.wsr_query_ald_link_key != "null" &&
          logRecord.op != "useless_openid"
        ) {
          val rowKey = logRecord.op.reverse + ":" + logRecord.ak
          val unionKey = rowKey.hashCode
          if (!linkCacheSet.contains(unionKey)) {
            val result = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, newUserHbaseColumn, newUserTableName)
            if (result._1 == "notStored") {
              logRecord.ifo = "true"
              //            }else if(result._1 == currentTime.toString){
              //              logRecord.ifo = "false"
              //            }else if(result._1.toLong < ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)){
              //              logRecord.ifo = "false"
            } else {
              logRecord.ifo = "false"
              //              logRecord.ifo = "true"
              insertTableWithName(rowKey, newUserHbaseColumn, currentTime, newUserTableName)
            }
            linkCacheSet.add(unionKey)
          } else {
            logRecord.ifo = "false"
          }
          recordsRdd += logRecord
        }
      })
      recordsRdd.iterator
    })
    val cacheSet: Set[Int] = Set()
    val amountRDD = logRecordRdd.mapPartitions(par => {
      val recordsRdd = ArrayBuffer[LogRecord]()
      par.foreach(logRecord => {
        if (logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.op) &&
          logRecord.op != "useless_openid"
        ) {
          if (logRecord.wsr_query_ald_link_key == "null" || StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key)) {
            val rowKey = logRecord.op.reverse + ":" + logRecord.ak
            val unionKey = (rowKey).hashCode
            if (!cacheSet.contains(unionKey)) {
              val result = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, newUserHbaseColumn, newUserTableName)
              if (result._1 == "notStored") {
                logRecord.ifo = "true"
                //              } else if (result._1 == currentTime.toString) {
                //                logRecord.ifo = "false"
                //              } else if (result._1.toLong < ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)) {
                //                logRecord.ifo = "false"
              } else {
                logRecord.ifo = "false"
                //                logRecord.ifo = "true"
                insertTableWithName(rowKey, newUserHbaseColumn, currentTime, newUserTableName)
              }
              cacheSet.add(unionKey)
            } else {
              logRecord.ifo = "false"
            }
            recordsRdd += logRecord
          }
        }
      })
      recordsRdd.iterator
    })
    linkRDD.union(amountRDD)
  }


  def handleNewUserForOldSDKShare(rdd: RDD[LogRecord], baseRedisKey: String, prefix: String): Unit = {
    import scala.collection.mutable.Set
    val cacheSet: Set[Int] = Set()
    rdd.foreachPartition(par => {
      val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) {
        resource =>
          par.foreach(logRecord => {
            //            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              logRecord.at != null &&
              logRecord.et != null &&
              logRecord.ifo == "true" &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.uu) &&
              logRecord.v != null
              && SDKVersionUtils.isOldSDk(logRecord.v)) {
              val unionKey = (logRecord.ak + logRecord.uu).hashCode
              val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
              if (!cacheSet.contains(unionKey)) {
                val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

                if (resource.exists(final_key) == false) {
                  //如果不存在则添加
                  resource.set(final_key, "true")
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                }
                cacheSet.add(unionKey)
              }
            }
            //存储用户的性别和城市
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
              if (StringUtils.isNotBlank(logRecord.city)) {
                resource.set(srcKey, (logRecord.city + ":" + logRecord.gender))
                resource.expireAt(srcKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            }
          })
      }
    })
  }

  /**
   * 一个新用户会话中如果包含openId ！= null 的数据，那么就将openId存储到redis中
   *
   * @param baseRedisKey
   */
  def handleOpenId(rdd: RDD[ConsumerRecord[String, String]], baseRedisKey: String, prefix: String): Unit = {
    import scala.collection.mutable.Set
    val cacheSet: Set[Int] = Set()
    rdd.foreachPartition(par => {
      val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) {
        resource =>
          par.foreach(line => {
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              logRecord.at != null &&
              logRecord.et != null &&
              logRecord.op != null &&
              logRecord.ifo == "true" &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.uu)) {
              val unionKey = (logRecord.ak + logRecord.op).hashCode
              val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
              if (!cacheSet.contains(unionKey)) {
                val expand_Key = s"$baseRedisKey:$d:openId" + ":" + logRecord.at.trim //展开后的key
                val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
                val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

                if (resource.exists(final_key) == false) {
                  //如果不存在则添加
                  resource.set(final_key, logRecord.op)
                  resource.expireAt(final_key, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  cacheSet.add(unionKey)
                }
                cacheSet.add(unionKey)
              }
            }
          })
      }
    })
  }

  /**
   * 标记 新用户会话中的openId，因为老算法只会计算新用户下的授权用户
   * 使用前边存储的openid将会话中所有的op赋值，防止后续计算新增授权用户的时候同一个会话今日两种计算逻辑
   *
   * @param logRecord
   * @param resource
   * @param baseRedisKey
   * @return
   */
  def markOpenId(logRecord: LogRecord, resource: BaseRedisCache, baseRedisKey: String): LogRecord = {
    val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
    val expand_Key = s"$baseRedisKey:$d:openId" + ":" + logRecord.at.trim //展开后的keyx
    val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
    val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

    val openId = resource.get(final_key)
    if (StringUtils.isNotBlank(openId)) {
      logRecord.op = openId
    }
    logRecord
  }

  /**
   * 标记 新用户
   *
   * @param logRecord
   * @param resource
   * @param baseRedisKey
   * @return
   */
  def markNewUser(logRecord: LogRecord, resource: BaseRedisCache, baseRedisKey: String): LogRecord = {
    val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
    val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim //展开后的keyx
    val expand_Key_hash = HashUtils.getHash(expand_Key).toString //对整个key取hash值
    val final_key = expand_Key_hash + ":" + expand_Key //将完整的key，取hash值并前置，得到最终的存入redis的key

    val ifo = resource.get(final_key)
    if (StringUtils.isNotBlank(ifo)) {
      logRecord.ifo = ifo
    }
    logRecord
  }

  /**
   * 缓存各个小程序，各个模块的上线状态
   *
   * @param sql_param 模块上线状态
   * @return
   */
  def cacheGrapUser(sql_param: String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            s"""
               |select ak,$sql_param
               |from aldstat_grey_ak
            """.stripMargin)
          while (rs.next()) {
            val ak = rs.getString(1)
            val online_status = rs.getInt(2)
            if (ak != null && online_status != null)
              map.put(ak.toString, online_status.toString)
          }
      }
    }
    map
  }

  /**
   * 判断是灰度状态，还是上线状态
   * true:上线状态
   * false:灰度状态
   *
   * @return
   */
  def isOnline(): Boolean = {
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            """
              |SELECT online_status
              |FROM aldstat_online_status where online_status = 1
            """.stripMargin)
          if (rs.next()) {
            true
          } else {
            false
          }
      }
    }
  }

  /**
   * 分模块判断上线状态
   * true:上线状态
   * false:灰度状态
   *
   * @param online_status
   * @return
   */
  def isOnline(online_status: String): Boolean = {
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            s"""
               |SELECT $online_status
               |FROM aldstat_online_status where $online_status = 1
            """.stripMargin)
          if (rs.next()) {
            true
          } else {
            false
          }
      }
    }
  }

}
