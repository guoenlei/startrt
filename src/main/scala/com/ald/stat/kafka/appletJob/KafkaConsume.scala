package com.ald.stat.kafka.appletJob

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date

import com.ald.stat.cache.{AbstractRedisCache, CacheRedisFactory}
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, RedisUtils}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 2018/5/18.
  */
object KafkaConsume {

  val logger = LoggerFactory.getLogger("KafkaConsume")
  //分步执行的批次记录数量
  var patchBatchNum: String = ConfigUtils.getProperty("patch.batch.num")

  /**
    * 实时从最新offset开始
    *
    * @param ssc
    * @param group
    * @param topic
    * @return
    */
  def getStream(ssc: StreamingContext, group: String, topic: String): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Set(topic)

    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    dStream
  }

  /**
    * 从指定offset开始
    *
    * @param ssc
    * @param group
    * @param topic
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def streamFromOffsets(ssc: StreamingContext, group: String, topic: String, baseRedisKey: String, redisPrefix: String): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val brokerHost = ConfigUtils.getProperty("kafka.host")
    val d = ComputeTimeUtils.getDayAndHourStr(new Date())
    lazy val redisCache = CacheRedisFactory.getInstances(redisPrefix).asInstanceOf[AbstractRedisCache]

    try{
      use(redisCache.getResource) {
        resource => {
          var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions")
          if (StringUtils.isBlank(partitionList)) {
            partitionList = "0,1"
          }
          val map = getWithStartOffset(brokerHost, topic, partitionList)
          var topicPartitionToOffset = Map[TopicPartition, Long]()
          if (map != null && !map.isEmpty) {
            map.foreach(k => {
              val topic = k._1.split(",")(0)
              val partition = k._1.split(",")(1)
              val realEndOffset = k._2.split(",")(0) //kafka中最大的offset

              // 获取offset
              val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")
              println(s"latestOffsetKey:$latestOffsetKey")
              var cacheOffset = resource.get(latestOffsetKey)
              val realStartOffset = k._2.split(",")(1) //kafka中最小的offset
              //矫正 offset ->cacheOffset 为空 或cacheOffset 大于 realEndOffset
              if (StringUtils.isBlank(cacheOffset)) {
                cacheOffset = realEndOffset
              } else {
                if (cacheOffset.toLong > realEndOffset.toLong) {
                  cacheOffset = realEndOffset.toString
                }
              }
              resource.set(latestOffsetKey, cacheOffset)
              println(s"pass date:$d,topic:$topic,$group,$partition,current from:$realStartOffset,real from:$cacheOffset")
              topicPartitionToOffset.+=((new TopicPartition(topic, partition.toInt), cacheOffset.toLong))
            })
          } else {
            System.exit(0)
          }
          KafkaUtils.createDirectStream(ssc, PreferConsistent, Assign[String, String](topicPartitionToOffset.keys, kafkaParams, topicPartitionToOffset))
        }
      }
    }finally {
      if (redisCache != null) redisCache.close()
    }

  }

  /**
    * 从指定offset开始
    *
    * @param ssc
    * @param group
    * @param topic
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def streamFromOffsetsDebug(ssc: StreamingContext, group: String, topic: String, baseRedisKey: String, redisPrefix: String): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val brokerHost = ConfigUtils.getProperty("kafka.host")
    val d = ComputeTimeUtils.getDayAndHourStr(new Date())
    lazy val redisCache = CacheRedisFactory.getInstances(redisPrefix).asInstanceOf[AbstractRedisCache]

    try{
      use(redisCache.getResource) {
        resource => {
          var partitionList = ConfigUtils.getProperty("kafka.raw.debug.topic.partitions")
          if (StringUtils.isBlank(partitionList)) {
            partitionList = "0,1"
          }
          val map = getWithStartOffset(brokerHost, topic, partitionList)
          var topicPartitionToOffset = Map[TopicPartition, Long]()
          if (map != null && !map.isEmpty) {
            map.foreach(k => {
              val topic = k._1.split(",")(0)
              val partition = k._1.split(",")(1)
              val realEndOffset = k._2.split(",")(0) //kafka中最大的offset

              // 获取offset
              val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")
              var cacheOffset = resource.get(latestOffsetKey)
              println(s"latestOffsetKey:$latestOffsetKey")
              val realStartOffset = k._2.split(",")(1) //kafka中最小的offset
              //矫正 offset ->cacheOffset 为空 或cacheOffset 大于 realEndOffset
              if (StringUtils.isBlank(cacheOffset)) {
                cacheOffset = realEndOffset
              } else {
                if (cacheOffset.toLong > realEndOffset.toLong) {
                  cacheOffset = realEndOffset.toString
                }
              }
              resource.set(latestOffsetKey, cacheOffset)
              println(s"pass date:$d,topic:$topic,$group,$partition,current from:$realStartOffset,real from:$cacheOffset")
              topicPartitionToOffset.+=((new TopicPartition(topic, partition.toInt), cacheOffset.toLong))
            })
          } else {
            System.exit(0)
          }
          KafkaUtils.createDirectStream(ssc, PreferConsistent, Assign[String, String](topicPartitionToOffset.keys, kafkaParams, topicPartitionToOffset))
        }
      }
    }finally {
      if (redisCache != null) redisCache.close()
    }

  }


  /**
    * 直接进行获取rdd
    *
    * @param sc
    * @param topic
    * @param group
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def rddFromOffsetRangeToPatch(sc: SparkContext, topic: String, group: String, baseRedisKey: String, redisPrefix: String): RDD[ConsumerRecord[String, String]]= {
    val kafkaParams = new util.HashMap[String, Object]()
    val brokerHost = ConfigUtils.getProperty("kafka.host")
    kafkaParams.put("bootstrap.servers", brokerHost)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", group)
    kafkaParams.put("auto.offset.reset", "earliest")
    kafkaParams.put("enable.auto.commit", (false: java.lang.Boolean))

    val d = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions")
    if (StringUtils.isBlank(partitionList)) {
      partitionList = "0,1"
    }
    lazy val redisCache = CacheRedisFactory.getInstances(redisPrefix).asInstanceOf[AbstractRedisCache]
    use(redisCache.getResource) {
      resource => {
        val map = getWithStartOffset(brokerHost, topic, partitionList)//获取kafka中的offset信息
        var topicPartitionToOffset = ArrayBuffer[OffsetRange]()
        if (map != null && !map.isEmpty){
          map.foreach(k => {
            val topic = k._1.split(",")(0)
            val partition = k._1.split(",")(1)
            val realMaxOffset = k._2.split(",")(0)
            val realMinOffset = k._2.split(",")(1)
            println("partition:" + partition + ",realMaxOffset:" + realMaxOffset + ",realMinOffset:" + realMinOffset)

            val minOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "min")//缓存中的最小offset的key
            val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")//缓存中的最新offset的key
            var patchMinOffset = resource.get(minOffsetKey)//补偿用的最小的offset
            var patchLatestOffset = resource.get(latestOffsetKey)//补偿用的最大的offset

            if(StringUtils.isNotBlank(patchLatestOffset)){
              if (realMaxOffset.toLong > patchLatestOffset.toLong){ //如果真实的最大的offset大于缓存中最大的offset
                patchLatestOffset = realMaxOffset //此时补偿到真实的最大的offset
              }
            }else{
              patchLatestOffset = realMaxOffset
            }
            resource.set(latestOffsetKey,patchLatestOffset)//把补偿后，最大的offset放入缓存
            resource.expireAt(latestOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期


            val r = OffsetRange.create(new TopicPartition(topic, partition.toInt), patchMinOffset.toLong, patchLatestOffset.toLong)
            println(s"pass date:$d,topic:$topic,$group,$partition,from:$patchMinOffset,end:$patchLatestOffset")
            topicPartitionToOffset.+=(r)
          })
        } else {
          System.exit(0)
        }
        val offsetRanges = topicPartitionToOffset.toArray
        KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, PreferConsistent)
      }
    }
  }


  /**
    * 直接进行获取rdd
    *
    * @param sc
    * @param topic
    * @param group
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def rddFromOffsetRangeToPatchNew(sc: SparkContext, topic: String, group: String, baseRedisKey: String, redisPrefix: String,patchHour:String): RDD[ConsumerRecord[String, String]]= {
    val kafkaParams = new util.HashMap[String, Object]()
    val brokerHost = ConfigUtils.getProperty("kafka.host")
    kafkaParams.put("bootstrap.servers", brokerHost)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", group)
    kafkaParams.put("auto.offset.reset", "earliest")
    kafkaParams.put("enable.auto.commit", (false: java.lang.Boolean))

    val d = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + patchHour
    val dateStr = ComputeTimeUtils.getDayAndHourStr(new Date())
    var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions")
    if (StringUtils.isBlank(partitionList)) {
      partitionList = "0,1"
    }
    lazy val redisCache = CacheRedisFactory.getInstances(redisPrefix).asInstanceOf[AbstractRedisCache]
    use(redisCache.getResource) {
      resource => {
        val map = getWithStartOffset(brokerHost, topic, partitionList)//获取kafka中的offset信息
        var topicPartitionToOffset = ArrayBuffer[OffsetRange]()
        if (map != null && !map.isEmpty){
          map.foreach(k => {
            val topic = k._1.split(",")(0)
            val partition = k._1.split(",")(1)
            val realMaxOffset = k._2.split(",")(0)
            val realMinOffset = k._2.split(",")(1)
            println("partition:" + partition + ",realMaxOffset:" + realMaxOffset + ",realMinOffset:" + realMinOffset)

            val minOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "min")//缓存中的最小offset的key
            val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")//缓存中的最新offset的key
            val latestOffsetKeyNow = RedisUtils.buildOffsetKey(dateStr, topic, group, partition.toInt, "latest")//缓存中的最新offset的key
            var patchMinOffset = resource.get(minOffsetKey)//补偿用的最小的offset
            var patchLatestOffset = resource.get(latestOffsetKey)//补偿用的最大的offset

            if(StringUtils.isNotBlank(patchLatestOffset)){
              if (realMaxOffset.toLong > patchLatestOffset.toLong){ //如果真实的最大的offset大于缓存中最大的offset
                patchLatestOffset = realMaxOffset //此时补偿到真实的最大的offset
              }
            }else{
              patchLatestOffset = realMaxOffset
            }
            resource.set(latestOffsetKeyNow,patchLatestOffset)//把补偿后，最大的offset放入缓存
            resource.expireAt(latestOffsetKeyNow,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期


            val r = OffsetRange.create(new TopicPartition(topic, partition.toInt), patchMinOffset.toLong, patchLatestOffset.toLong)
            println(s"pass date:$d,topic:$topic,$group,$partition,from:$patchMinOffset,end:$patchLatestOffset")
            topicPartitionToOffset.+=(r)
          })
        } else {
          System.exit(0)
        }
        val offsetRanges = topicPartitionToOffset.toArray
        KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, PreferConsistent)
      }
    }
  }


  /**
    * 每批次补偿1亿
    *
    * @param sc
    * @param topic
    * @param group
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def rddFromOffsetRangeToPatchBatch(sc: SparkContext, topic: String, group: String, baseRedisKey: String, redisPrefix: String,patchHour:String): (RDD[ConsumerRecord[String, String]], Boolean)= {
    val kafkaParams = new util.HashMap[String, Object]()
    val brokerHost = ConfigUtils.getProperty("kafka.host")
    kafkaParams.put("bootstrap.servers", brokerHost)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", group)
    kafkaParams.put("auto.offset.reset", "earliest")
    kafkaParams.put("enable.auto.commit", (false: java.lang.Boolean))
    //判断是否需要退出
    var exit = false
    val d = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + patchHour
    val dateStr = ComputeTimeUtils.getDayAndHourStr(new Date())
    var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions")
    if (StringUtils.isBlank(partitionList)) {
      partitionList = "0,1"
    }
    lazy val redisCache = CacheRedisFactory.getInstances(redisPrefix).asInstanceOf[AbstractRedisCache]
    use(redisCache.getResource) {
      resource => {
        val map = getWithStartOffset(brokerHost, topic, partitionList)//获取kafka中的offset信息
        var topicPartitionToOffset = ArrayBuffer[OffsetRange]()
        if (map != null && !map.isEmpty){
          map.foreach(k => {
            val topic = k._1.split(",")(0)
            val partition = k._1.split(",")(1)
            val realMaxOffset = k._2.split(",")(0)
            val realMinOffset = k._2.split(",")(1)
            println("partition:" + partition + ",realMaxOffset:" + realMaxOffset + ",realMinOffset:" + realMinOffset)

            val minOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "min")//缓存中的最小offset的key
            println("minOffsetKey =========" + minOffsetKey)
            val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")//缓存中的最新offset的key
            val latestOffsetKeyNow = RedisUtils.buildOffsetKey(dateStr, topic, group, partition.toInt, "latest")//缓存中的最新offset的key
            var patchMinOffset = resource.get(minOffsetKey)//补偿用的最小的offset
            var patchLatestOffset = (patchMinOffset.toLong + 4000000).toString
//            var patchLatestOffset = resource.get(latestOffsetKey)//补偿用的最大的offset


              if (realMaxOffset.toLong < patchLatestOffset.toLong){ //如果真实的最大的offset小于批次中最大的offset
                patchLatestOffset = realMaxOffset //此时补偿到真实的最大的offset
                resource.set(latestOffsetKeyNow,patchLatestOffset)//把补偿后，最大的offset放入缓存
                resource.expireAt(latestOffsetKeyNow,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                exit = true
                //补偿结束
              }
            else{
                resource.set(minOffsetKey,patchLatestOffset)//把补偿一亿后，最小的offset放入缓存
                resource.expireAt(minOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              exit = false
                //单批次补偿结束，准备进入下一个批次，从本次存储的最小偏移量开始
            }


            val r = OffsetRange.create(new TopicPartition(topic, partition.toInt), patchMinOffset.toLong, patchLatestOffset.toLong)
            println(s"pass date:$d,topic:$topic,$group,$partition,from:$patchMinOffset,end:$patchLatestOffset")
            topicPartitionToOffset.+=(r)
          })
        } else {
          System.exit(0)
        }
        val offsetRanges = topicPartitionToOffset.toArray
        (KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, PreferConsistent),exit)
      }
    }
  }

  /**
    * 直接进行获取rdd
    *
    * @param sc
    * @param topic
    * @param group
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def rddFromOffsetRange(sc: SparkContext, topic: String, group: String, baseRedisKey: String, redisPrefix: String): (RDD[ConsumerRecord[String, String]], Boolean) = {

    val kafkaParams = new util.HashMap[String, Object]()
    val brokerHost = ConfigUtils.getProperty("kafka.host")
    kafkaParams.put("bootstrap.servers", brokerHost)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", group)
    kafkaParams.put("auto.offset.reset", "earliest")
    kafkaParams.put("enable.auto.commit", (false: java.lang.Boolean))

    //判断是否需要退出
    val exit = false
    val d = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val donePartitionsKey = RedisUtils.buildDonePartitionsKey(d, topic, group, "donePartitions")
    var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions")
    if (StringUtils.isBlank(partitionList)) {
      partitionList = "0,1"
    }

    lazy val redisCache = CacheRedisFactory.getInstances(redisPrefix).asInstanceOf[AbstractRedisCache]
    try{
      use(redisCache.getResource) {
        resource => {
          var donePartitions = resource.smembers(donePartitionsKey)
          if (donePartitions == null) {
            donePartitions = new util.HashSet[String]()
          }
          //判断是否所有分区都已经处理完毕
          if (partitionList.split(",").size == donePartitions.size()) {
            println("donePartitions：" + donePartitions)
            return (null, true) //如果所有分区都处理完毕了，则返回true,令补偿程序退出
          }
          val map = getWithStartOffset(brokerHost, topic, partitionList)
          var topicPartitionToOffset = ArrayBuffer[OffsetRange]()

          if (map != null && !map.isEmpty) {
            map.filter(k => {
              val partition = k._1.split(",")(1)
              if (!donePartitions.contains(partition.toString)) {
                true
              } else {
                false
              }
            }).foreach(k => {
              val topic = k._1.split(",")(0)
              val partition = k._1.split(",")(1)
              val realMaxOffset = k._2.split(",")(0)
              val realMinOffset = k._2.split(",")(1)
              println("partition:" + partition + ",realMaxOffset:" + realMaxOffset + ",realMinOffset:" + realMinOffset)

              // 获取offset
              //这里需要进行手动分批计算，保存一个step offset，直到最后一个 step offset 和latest offset一样的时候，结束本次partition计算
              val stepLatestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "step:latest")
              val stepLatestOffset = resource.get(stepLatestOffsetKey)
              if (StringUtils.isBlank(patchBatchNum)) {
                patchBatchNum = "500000"
              }
              //上限
              var stepMinOffsetVal = 0l
              //下限
              var stepLatestOffsetVal = 0l

              if (StringUtils.isBlank(stepLatestOffset)) {
                val minOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "min")
                var minOffset = resource.get(minOffsetKey)
                println("cacheMinOffset:" + minOffset)
                if (StringUtils.isBlank(minOffset)) {
                  minOffset = realMinOffset
                  //更新redis
                  resource.set(minOffsetKey, minOffset)
                } else {
                  if (minOffset.toLong < realMinOffset.toLong) {// TODO: 这里有问题 ，真实的最小的offset 大于 cacheMinOffset
                    //minOffset = realMinOffset
                    //更新redis
                    resource.set(minOffsetKey, minOffset)
                  }
                }
                stepMinOffsetVal = minOffset.toLong
              } else {
                stepMinOffsetVal = stepLatestOffset.toLong
              }
              stepLatestOffsetVal = stepMinOffsetVal + patchBatchNum.toLong
              //下限
              val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")
              var latestOffset = resource.get(latestOffsetKey)
              if (StringUtils.isBlank(latestOffset)) {
                latestOffset = realMaxOffset
                //矫正
                resource.set(latestOffsetKey, latestOffset)
              } else {
                //TODO:这里存在补偿缓存上限小于下限
                //缓存中的最大offset比真实最小的offset还小,则把缓存offset矫正为最大offset
                if (latestOffset.toLong < realMinOffset.toLong) {
                  latestOffset = realMaxOffset
                  //矫正
                  resource.set(latestOffsetKey, latestOffset)
                }
              }
              //判断是否需要进行退出
              if (stepLatestOffsetVal > latestOffset.toLong) {
                stepLatestOffsetVal = latestOffset.toLong
                //加入完成的列表
                resource.sadd(donePartitionsKey, partition.toString)
                resource.expireAt(donePartitionsKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
              //步位保存到redis
              resource.set(stepLatestOffsetKey, stepLatestOffsetVal.toString)
              resource.expireAt(stepLatestOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              val r = OffsetRange.create(new TopicPartition(topic, partition.toInt), stepMinOffsetVal, stepLatestOffsetVal)
              println(s"pass date:$d,topic:$topic,$group,$partition,from:$stepMinOffsetVal,end:$stepLatestOffsetVal")
              topicPartitionToOffset.+=(r)
            })
          } else {
            System.exit(0)
          }
          val offsetRanges = topicPartitionToOffset.toArray
          (KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, PreferConsistent), exit)
        }
      }
    }finally {
      if (redisCache != null) redisCache.close()
    }

  }

  /**
    *
    * @param brokerList
    * @param topic
    * @param partitionList
    * @return
    */
  def getWithStartOffset(brokerList: String, topic: String, partitionList: String): Map[String, String] = {

    val clientId = "GetOffset"
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)

    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, "trendConsume", 10000).topicsMetadata
    if (topicsMetadata.size != 1 || !topicsMetadata(0).topic.equals(topic)) {
      System.err.println(("Error: no valid topic metadata for topic: %s, " + " probably the topic does not exist, run ").format(topic) +
        "kafka-list-topic.sh to verify")
      System.exit(1)
    }
    var topicPartitionToOffset = Map[String, String]()
    val partitions =
      if (partitionList == "") {
        topicsMetadata.head.partitionsMetadata.map(_.partitionId)
      } else {
        partitionList.split(",").map(_.toInt).toSeq
      }

    println("partitionList:" + partitionList)

    partitions.foreach { partitionId =>
      val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partitionId)
      partitionMetadataOpt match {
        case Some(metadata) =>
          metadata.leader match {
            case Some(leader) =>
              val consumer = new SimpleConsumer(leader.host, leader.port, 10000, 100000, clientId)
              val topicAndPartition = TopicAndPartition(topic, partitionId)
              val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(-1, 2)))
              val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets
              var max, min = 0l
              if (offsets != null) {
                if (offsets.length == 2) {
                  max = offsets(0)
                  min = offsets(1)
                }
              }
              println(s"current: topic:${topicAndPartition.topic},${topicAndPartition.partition},$max,$min")
              topicPartitionToOffset.+=((s"${topicAndPartition.topic},${topicAndPartition.partition}", s"$max,$min"))
            case None => System.err.println("Error: partition %d does not have a leader. Skip getting offsets".format(partitionId))
          }
        case None => System.err.println("Error: partition %d does not exist".format(partitionId))
      }
    }
    topicPartitionToOffset
  }
}