package com.ald.stat.appletJob.offline.independentLink.cheating

import java.util.{Date, Properties}

import com.ald.stat.appletJob.task.DailyAnalysisTrendTask.sendToKafka
import com.ald.stat.utils.HbaseUtiles.insertTableWithName
import com.ald.stat.utils._
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object AnalysisIndependentLinkException {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val logger = LoggerFactory.getLogger("adLinkException")
    logger.info("广告异常监测开始执行{}", System.currentTimeMillis())
    logger.info("广告异常监测参数{}", args.mkString(","))
  val spark = SparkSession.builder()
    .config("spark.sql.planner.skewJoin", "true")
    .config("spark.sql.planner.skewJoin.threshold", "500000")
    .config("spark.debug.maxToStringFields",100)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
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
      spark.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    ArgsTool.analysisArgs(args)
    val day = ArgsTool.day

    val schema = StructType(List(
      StructField("ak", StringType, true),
      StructField("op", StringType, true),
      StructField("img", StringType, true),
      StructField("client_ip", StringType, true),
      StructField("wsr_query_ald_link_key", StringType, true),
      StructField("v", StringType, true)
    )
    )
    val authSchema = StructType(List(
      StructField("ak", StringType, true),
      StructField("op", StringType, true),
      StructField("client_ip", StringType, true),
      StructField("authStatus", StringType, true)
    )
    )
    //读取parquet数据
    val logs = ArgsTool.getDFOfDay(spark, "hdfs://10.0.100.17:4007/ald_log_parquet", ArgsTool.day, schema)
        .filter("ak != '' and op != '' and op != 'null' and v >= '7.3.0'")
    logs.createOrReplaceTempView("logs")
    //计算异常
    val baseLogs = spark.sql(
      """
        |SELECT ak,op,img,client_ip
        |FROM logs
        |WHERE wsr_query_ald_link_key!='null' and wsr_query_ald_link_key !=''
        |GROUP BY ak,op,img,client_ip
      """.stripMargin)
    baseLogs.cache()
    baseLogs.createOrReplaceTempView("baseLogs")
    val authRDD = handleAuthUser(baseLogs)
    spark.createDataFrame(authRDD, authSchema).createOrReplaceTempView("authLogs")
    //计算每个ip下的授权数

    spark.sql(
      """
        |SELECT ak,client_ip,count(distinct(op)) authCount
        |FROM authLogs
        |WHERE authStatus = '0'
        |GROUP by ak,client_ip
      """.stripMargin).createOrReplaceTempView("auth_count")

    spark.sql(
      """
        |SELECT ak,client_ip,authCount
        |FROM auth_count
        |WHERE authCount > 10
      """.stripMargin).createOrReplaceTempView("exception_auth")
    //计算所有ip下的uv
     spark.sql(
      """
        |SELECT ak,client_ip,count(distinct(op)) uv
        |FROM baseLogs
        |GROUP BY ak,client_ip
      """.stripMargin).createOrReplaceTempView("uv_tmp")
    //最终结果集
    val resultDF = spark.sql(
      """
        |SELECT t1.ak,t1.client_ip,t1.authCount,t2.uv
        |FROM exception_auth t1
        |LEFT JOIN uv_tmp t2
        |ON t1.ak = t2.ak and t1.client_ip = t2.client_ip
      """.stripMargin)
    insert2db(resultDF, spark, kafkaProducer)
    spark.sqlContext.uncacheTable("baseLogs")
    spark.close()
  }

  def insert2db(resultDF: sql.DataFrame, spark: SparkSession, kafkaProducer: Broadcast[KafkaSink[String, String]]): Unit = {
    val day = ArgsTool.day
    resultDF.foreachPartition(rows => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      rows.foreach(row => {
        val app_key = row.get(0)
        val client_ip = row.get(1)
        val abnormal_auth_count = row.get(2)
        val abnormal_vistor_count = row.get(3)
        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_link_cheat_protect
             |(
             |app_key,
             |day,
             |client_ip,
             |abnormal_auth_count,
             |abnormal_vistor_count,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$client_ip",
             | "$abnormal_auth_count",
             | "$abnormal_vistor_count",
             | now()
             | )
             | on duplicate key update
             | abnormal_auth_count="$abnormal_auth_count",
             | abnormal_vistor_count="$abnormal_vistor_count",
             | update_at = now()
          """.stripMargin
        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
    //spark.close()
  }
  /**
    * 通过habase标记新用户
    * @param logs 数据源
    * @return
    */
  def handleAuthUser(logs: sql.DataFrame): RDD[Row] = {
    val authUserHbaseColumn = "authStatus"
//    val authUserTableName = "STAT.ALDSTAT_ADVERTISE_AUTH_TEST"//测试
    val authUserTableName = "STAT.ALDSTAT_ADVERTISE_AUTH"
    val day_wz = ArgsTool.day
    val dateLong = ComputeTimeUtils.tranTimeToLongNext(day_wz)
    //currentTime:存入Hbase的列值，这个时间要取传入的参数day为同一天
    val currentTime = (new Date()).getTime - (((new Date()).getTime - dateLong)/86400000)*86400000
    println("currentTime=====================" + currentTime)
    import scala.collection.mutable.Set
    val cacheSet: Set[Int] = Set()
    val rowRdd = logs.toJSON.rdd.map(line => {
      var result_arr = ""
      var result = ""
      if(line != null){
        val line_json = JSON.parseObject(JSON.parseObject(line.toString).toString)
        if(line_json.getString("ak") != "null" &&
          line_json.getString("ak") != "" &&
          line_json.getString("ak") != null &&
          line_json.getString("op") != "null" &&
          line_json.getString("op") != "" &&
          line_json.getString("op") != null &&
          line_json.getString("img") != "null" &&
          line_json.getString("img") != "" &&
          line_json.getString("img") != null &&
          line_json.getString("client_ip") != "null"&&
          line_json.getString("client_ip") != null&&
          line_json.getString("client_ip") != ""){
          val ak = line_json.get("ak").toString
          val op = line_json.get("op").toString
            //1代表已授权； 0代表未授权  默认已授权
          var authStatus = "1"
          val client_ip = line_json.get("client_ip").toString
          val rowKey = op.reverse + ":" + ak
          val unionKey = (rowKey).hashCode
          if(!cacheSet.contains(unionKey)) {
            val result = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, authUserHbaseColumn, authUserTableName)
            if(result._1 == "notStored"){
              authStatus = "0"
            }else if(result._1 == currentTime.toString){
              authStatus = "1"
            }else if(result._1.toLong < ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)){
              authStatus = "1"
            }else{
              authStatus = "0"
              insertTableWithName(rowKey, authUserHbaseColumn, currentTime, authUserTableName)
            }
            cacheSet.add(unionKey)
          }else{
            authStatus = "1"
          }
          result = s"$ak---$op---$client_ip---$authStatus"
          result_arr += result + "#--#"
        }
      }
      result_arr
    }).flatMap(_.split("#--#")).filter(x=>(x.size > 0)).map(line => {
      val line_arr = line.split("---")
      if (line_arr.length == 4) {
        Row(line_arr(0), line_arr(1), line_arr(2), line_arr(3))
      } else {
        Row("", "", "", "")
      }
    })
    rowRdd//.union(amountRowRdd)
  }
}
