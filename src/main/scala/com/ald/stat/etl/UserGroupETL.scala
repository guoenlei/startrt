package com.ald.stat.etl

import java.net.URI
import java.util.Date

import com.ald.stat.cache.{AbstractRedisCache, BaseRedisCache, CacheRedisFactory}
import com.ald.stat.etl.model.{JsonRecord, ParquetRecord}
import com.ald.stat.etl.util.RecordConverUtil
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, HashUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 用户分群的ETL操作
  * 放在每日凌晨之后执行，对昨日的json数据进行清洗，为用户分群提供数据
  * 注意：需要确保程序执行之前，前一天的所有json文件均已生成
  * Created by zhaofw on 2018/8/25.
  */
object UserGroupETL {
  val baseRedisKey = "etl"
  var dateStr = ComputeTimeUtils.getYesterDayStr(new Date(),"yyyyMMdd")//默认去昨日的日期
  var hour = "00"//默认读取00点的数据
  val pathPrefix = "ald-log9"
  val initial_path = "hdfs://10.0.100.17:4007/ald_jsonlogs"
  val write_path = "hdfs://10.0.0.87:4007/ald_userGroup" //84集群
  val redis_prefix = "groupETL"

  //其他的分组ID
  val otherSceneId = ConfigUtils.getProperty("other.scene.id")
  val otherSceneGroupId = ConfigUtils.getProperty("other.scene.group.id")
  val unknownSceneGroupId = ConfigUtils.getProperty("unknown.scene.id")
  val unknownSceneId = ConfigUtils.getProperty("unknown.scene.group.id")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    val sceneMap = broadcastSceneMap(spark)//广播场景值
    val qrMap = broadcastQrMap(spark)//广播二维码
    val linkMap = broadcastLinkMap(spark)//广播外链
    val modelMap = broadcastModelMap(spark)//广播机型
    val brandMap = broadcastBrandMap(spark)//广播品牌
    if(args.length==2){
      dateStr = args(0)
      hour = args(1)
    }else{
      println("请输入日期和小时格式：yyyyMMdd HH")
    }

    val path = s"$initial_path/$dateStr"
    //val file_list = getFileList(path,pathPrefix)
    val logs_rdd = spark.sparkContext.textFile(s"$path/ald*${dateStr}${hour}*.json",200)
    logs_rdd.cache()
    loadJsonLogAndHandleNewUser(spark,redis_prefix,logs_rdd)//加载json数据，存储新用户的会话并记录
    loadJsonLogAndETL(spark,redis_prefix,logs_rdd,sceneMap,qrMap,linkMap,modelMap,brandMap)//再次加载数据，进行etl转换操作

    logs_rdd.unpersist()
    spark.stop()
  }

  /**
    * 加载json数据，并存储新用户的会话
    * 样例：hdfs://10.0.*00.**:*0*7/ald_jsonlogs/20180824/ald-log9.2018082418.1535107205412.json
    * @param spark
    * @param prefix
    */
  def loadJsonLogAndHandleNewUser(spark: SparkSession,prefix:String,logs_rdd: RDD[String])={
    logs_rdd.foreachPartition(par=>{
      val redisCache = CacheRedisFactory.getInstances("groupETL").asInstanceOf[AbstractRedisCache]
      use(redisCache.getResource) {resource=>
        par.foreach(line=>{
          val logRecord = JsonRecord.logBean(line)
          if (logRecord != null && logRecord.at != null && logRecord.et != null && logRecord.ifo == "true" ) {//&& logRecord.v < "7.0.0"
          val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
            val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim  //展开后的key
            val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
            val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key
            if (resource.exists(final_key) == false){//如果不存在则添加
              resource.set(final_key,"true")
              resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
            }
          }
        })
      }
    })
  }

  /**
    * 加载json日志，并完成ETL的各个操作
    * @param spark
    * @param prefix
    */
  def loadJsonLogAndETL(spark: SparkSession,prefix:String,logs_rdd: RDD[String],sceneMap: Broadcast[Map[String, String]],
                        qrMap: Broadcast[Map[String, String]], linkMap: Broadcast[Map[String, String]],
                        modelMap: Broadcast[Map[String, String]], brandMap:Broadcast[Map[String, String]])={
      val record_df = spark.createDataFrame(logs_rdd.mapPartitions(par=>{
        val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
        val recordsRdd = ArrayBuffer[Row]()
        use(redisCache.getResource) {resource=>
          par.foreach(line=>{
            val logRecord = JsonRecord.logBean(line)
            var parquetRecord = new ParquetRecord
            if(logRecord!=null&&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.at) &&
              StringUtils.isNotBlank(logRecord.ev) &&
              StringUtils.isNotBlank(logRecord.et) &&
              StringUtils.isNotBlank(logRecord.uu)){
              val row_list = RecordConverUtil.parseLog(logRecord,parquetRecord,resource,sceneMap,qrMap,linkMap,modelMap,brandMap)
              row_list.foreach(row=>{
                recordsRdd += row
              })
            }
          })
        }
        recordsRdd.iterator
      }),RecordConverUtil.structType)

      // TODO: 这里有优化空间
      record_df.repartition(5).write.parquet(s"$write_path/$dateStr/$hour")//写入parqu文件
  }

  /**
    * 标记新用户
    * @param logRecord
    * @param resource
    * @param baseRedisKey
    * @return
    */
  def markNewUser(logRecord: JsonRecord, resource: BaseRedisCache, baseRedisKey: String): JsonRecord = {
    val d = ComputeTimeUtils.getYesterDayStr(new Date(),"yyyyMMdd")
    val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim  //展开后的keyx
    val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
    val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key
    val ifo = resource.get(final_key)
    if (StringUtils.isNotBlank(ifo)) {
      logRecord.ifo = ifo
    }
    logRecord
  }

  /**广播场景值与场景值组的对应关系*/
  def broadcastSceneMap(spark: SparkSession): Broadcast[Map[String, String]] ={
    var sceneMap = Map[String, String]()
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
              sceneMap.+=((sid.toString, group_id.toString))
            }
          }
      }
    }
    spark.sparkContext.broadcast(sceneMap)
  }
  /**广播二维码组与二维码的对应关系*/
  def broadcastQrMap(spark: SparkSession): Broadcast[Map[String, String]] ={
    var qrMap = Map[String, String]()
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            """
              |select qr_key, qr_group_key
              |from ald_code
            """.stripMargin)
          while (rs.next()) {
            val qr_key = rs.getString(1)
            val qr_group_key = rs.getString(2)
            if (qr_key != null && qr_group_key != null) {
              qrMap.+=((qr_key.toString, qr_group_key.toString))
            }
          }
      }
    }
    spark.sparkContext.broadcast(qrMap)
  }
  /**广播link_key与link_media对应关系*/
  def broadcastLinkMap(spark: SparkSession): Broadcast[Map[String, String]] ={
    var linkMap = Map[String, String]()
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            """
              |select link_key,media_id
              |from ald_link_trace
              |where is_del=0
            """.
              stripMargin)
          while (rs.next()) {
            val link_key = rs.getString(1)
            val media_id = rs.getInt(2)
            if (link_key != null && media_id != null) {
              linkMap.+=((link_key.toString, media_id.toString))
            }
          }
      }
    }
    spark.sparkContext.broadcast(linkMap)
  }
  /**广播机型*/
  def broadcastModelMap(spark: SparkSession): Broadcast[Map[String, String]] ={
    var modelMap = Map[String, String]() //型号
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            """
              |select uname,brand,name
              |from phone_model
            """.stripMargin)
          while (rs.next()) {
            val uname = rs.getString(1)
            val name = rs.getString(3)
            if (uname != null && name != null) {
              modelMap.+= ((uname.toString, name.toString))
            }
          }
      }
    }
    spark.sparkContext.broadcast(modelMap)
  }
  /**广播品牌*/
  def broadcastBrandMap(spark: SparkSession): Broadcast[Map[String, String]] ={
    var brandMap = Map[String, String]() //品牌
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            """
              |select uname,brand,name
              |from phone_model
            """.stripMargin)
          while (rs.next()) {
            val uname = rs.getString(1)
            val brand = rs.getString(2)
            if (uname != null && brand != null) {
              brandMap.+=((uname.toString, brand.toString))
            }
          }
      }
    }
    spark.sparkContext.broadcast(brandMap)
  }

  /**
    * 获取文件列表
    * @param initial_path
    * @return
    */
  def getFileList(initial_path: String,pathPrefix:String): ArrayBuffer[String] = {
    val conf = new Configuration()
    val hdfs = FileSystem.get(new URI(initial_path), conf)
    val fs = hdfs.listStatus(new Path(initial_path))
    val arrayList = ArrayBuffer[String]()
    for (status <- fs) {
      val path = status.getPath.toString
      val dir: Array[String] = path.split("/")
      if (dir.last.split("[.]")(0) == pathPrefix) {
        arrayList+=dir.last
      }
    }
    arrayList
  }

}
