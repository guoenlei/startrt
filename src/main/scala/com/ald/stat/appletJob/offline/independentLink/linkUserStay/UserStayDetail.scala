package com.ald.stat.appletJob.offline.independentLink.linkUserStay

import java.sql.Statement
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.ald.stat.appletJob.task.DailyAnalysisTrendTask.sendToKafka
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils.HbaseUtiles.insertTableWithName
import com.ald.stat.utils._
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by lmw on 2019/06/15.
  * 用户留存详情计算
  * 功能点：
  * 1）正常的用户留存计算
  * 2）结合场景值，二维码，外链三个渠道，进行区分计算
  */
object UserStayDetail {
  lazy val DAYS_AFTER = Array(0, 1, 2, 3, 4, 5, 6, 7) //默认只计算1天，2天，3天，4天，5天，6天，7天天的留存
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.locality.wait", 0)
      .config("spark.sql.planner.skewJoin", "true")
      .config("spark.sql.planner.skewJoin.threshold", 500000)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
      .appName(this.getClass.getName)
      //.master("local[*]")
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

    //获取link_key，media_id和app_key的关系
    DBUtils.readFromMysql(spark, "ald_link_trace").createGlobalTempView("ald_link_trace")

    ArgsTool.analysisArgs(args)//装配参数
    val stat_day = ArgsTool.day
    val customSchema = StructType(Array(
      StructField("ak", StringType, true),
      StructField("op", StringType, true),
      StructField("ifo", StringType, true),
      StructField("scene", StringType, true),
      StructField("wsr_query_ald_link_key", StringType, true),
      StructField("pp", StringType, true),
      StructField("v", StringType, true)
      ))
    //得到基于parquet的df
    ArgsTool.getDFOfDay(spark, "hdfs://10.0.100.17:4007/ald_log_parquet",stat_day,customSchema).createOrReplaceTempView("originLogs")

    //得到基于df的df_new
    val logs = spark.sql("select ak,op,scene,wsr_query_ald_link_key from originLogs")
    logs.createOrReplaceTempView("logs")

    spark.sql(
      """
        |select ak, op from logs group by op, ak
      """.stripMargin).repartition(400).createOrReplaceTempView("base_logs")
    spark.sqlContext.cacheTable("base_logs")
//  spark.sql("SELECT * FROM base_logs").show()

    val date = new SimpleDateFormat("yyyy-MM-dd").parse(stat_day)
    for (day_after <- DAYS_AFTER){
      val day = ComputeTimeUtils.getDayStr(date, -day_after, "yyyy-MM-dd") //得到计算日期的前n天
      val logs_df_origin = ArgsTool.getDFOfDay(spark, "hdfs://10.0.100.17:4007/ald_log_parquet",day,customSchema)//加载数据
      //通过hbase标记新用户
      val a = handleNewUser(logs_df_origin, day)
      spark.sqlContext.createDataFrame(a,customSchema).createOrReplaceTempView("logs_df_origin")
      spark.sql("select ak,op,scene,wsr_query_ald_link_key,ifo from logs_df_origin where op!='' and op!='null' and wsr_query_ald_link_key!='' and wsr_query_ald_link_key!='null' and pp!='' and pp!='null' group by ak,op,scene,wsr_query_ald_link_key,ifo")
        .createOrReplaceTempView("logs_df_1")
      spark.sql(
        """
          |select trace.link_key wsr_query_ald_link_key,trace.app_key ak,trace.media_id wsr_query_ald_media_id,
          |case when scene in (1058,1035,1014,1038) then scene else '其它' end as ag_ald_position_id,
          |vp.op,vp.ifo
          |from
          |(
          |select link_key,app_key,media_id
          |from global_temp.ald_link_trace
          |where is_del=0
          |) trace
          |join logs_df_1 vp
          |on vp.wsr_query_ald_link_key=trace.link_key
        """.stripMargin)
        .repartition(400)
        .createOrReplaceTempView("logs_df")
      spark.sqlContext.cacheTable("logs_df")
      spark.sql("SELECT * FROM logs_df").show()
      userStayDefault(spark, day, day_after, kafkaProducer) //计算默认情况下的留存
      userStaySource(spark, day, day_after,"wsr_query_ald_link_key", kafkaProducer)
      userStaySource(spark, day, day_after,"wsr_query_ald_media_id", kafkaProducer)
      userStaySource(spark, day, day_after,"ag_ald_position_id", kafkaProducer)
      spark.sqlContext.uncacheTable("logs_df")
    }
    spark.sqlContext.uncacheTable("base_logs")
    spark.sqlContext.uncacheTable("logs_df")

    spark.stop()
  }

  /**
    * 通过habase标记新用户
    * @param logs 数据源
    * @return
    */
  def handleNewUser(logs: sql.DataFrame,day_wz:String): RDD[Row] = {
    println("标记新用户一次    日期==================" + day_wz)
    val newUserStayHbaseColumn = "newuser"
//    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID_TEST"//测试
    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID"
    val dateLong = ComputeTimeUtils.tranTimeToLongNext(day_wz)
    //currentTime:存入Hbase的列值，这个时间要取传入的参数day为同一天
    val currentTime = (new Date()).getTime - (((new Date()).getTime - dateLong)/86400000)*86400000
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
          line_json.getString("pp") != null &&
          line_json.getString("pp") != "" &&
          line_json.getString("pp") != "null" &&
          line_json.getString("scene") != "null" &&
          line_json.getString("scene") != "" &&
          line_json.getString("scene") != null &&
          line_json.getString("wsr_query_ald_link_key") != "null"&&
          line_json.getString("wsr_query_ald_link_key") != null&&
          line_json.getString("wsr_query_ald_link_key") != ""){
          val ak = line_json.get("ak").toString
          val op = line_json.get("op").toString
          val pp = line_json.get("pp").toString
          var ifo = "false"
          val v = "useless"
          val scene = line_json.get("scene").toString
          val wsr_query_ald_link_key = line_json.get("wsr_query_ald_link_key").toString
          val rowKey = op.reverse + ":" + ak
          val unionKey = (rowKey).hashCode
          if(!cacheSet.contains(unionKey)) {
            val result = HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, newUserStayHbaseColumn, newUserTableName)
            if(result._1 == "notStored"){
              ifo = "true"
            }else if(result._1 == currentTime.toString){
              ifo = "false"
            }else if(result._1.toLong < ComputeTimeUtils.getTodayMoningTimeStamp(day_wz)){
              ifo = "false"
            }else{
              ifo = "true"
              insertTableWithName(rowKey, newUserStayHbaseColumn, currentTime, newUserTableName)
            }
            cacheSet.add(unionKey)
          }else{
            ifo = "false"
          }
          result = s"$ak---$op---$ifo---$scene---$wsr_query_ald_link_key---$pp---$v"
          result_arr += result + "#--#"
        }
      }
      result_arr
    }).flatMap(_.split("#--#")).filter(x=>(x.size > 0)).map(line => {
        val line_arr = line.split("---")
        if (line_arr.length == 7) {
          Row(line_arr(0), line_arr(1), line_arr(2), line_arr(3), line_arr(4), line_arr(5), line_arr(6))
        } else {
          Row("", "", "", "", "", "", "")
        }
    })
    logs.toJSON.rdd.map(line => {
      if(line != null){
        val line_json = JSON.parseObject(JSON.parseObject(line.toString).toString)
        if(line_json.getString("ak") != "null" &&
          line_json.getString("ak") != "" &&
          line_json.getString("ak") != null &&
          line_json.getString("op") != "null" &&
          line_json.getString("op") != "" &&
          line_json.getString("op") != null &&
          line_json.getString("pp") != null &&
          line_json.getString("pp") != "" &&
          line_json.getString("pp") != "null" &&
          line_json.getString("scene") != "null" &&
          line_json.getString("scene") != "" &&
          line_json.getString("scene") != null
          ){
          if(line_json.getString("wsr_query_ald_link_key") == "null" || line_json.getString("wsr_query_ald_link_key") == "" || line_json.getString("wsr_query_ald_link_key") == null){
            val ak = line_json.get("ak").toString
            val op = line_json.get("op").toString
            val rowKey = op.reverse + ":" + ak
            val unionKey = (rowKey).hashCode
            if(!cacheSet.contains(unionKey)) {
              HbaseUtiles.getDataFromHTableWithName(rowKey, currentTime, newUserStayHbaseColumn, newUserTableName)
              cacheSet.add(unionKey)
            }
          }
        }
      }
    })
      rowRdd
  }
  /**
    * 计算默认情况下的留存（无渠道）
    * @param spark
    * @param stat_day
    * @param day_after
    */
  private def userStayDefault(spark: SparkSession, stat_day: String, day_after: Int, kafkaProducer: Broadcast[KafkaSink[String, String]]) = {



    /**
      * 新用户数
      */
    //取出所有的op
    println("取出所有的op")
    spark.sql(
      """
        |SELECT op, ak, ifo
        |FROM
        |logs_df
      """.stripMargin).createOrReplaceTempView("logs_df_all")
    //将同一个uu放到一个分区中
     spark.sql(
      """
        |SELECT op, ak
        |FROM
        | logs_df_all
        |WHERE
        | ifo = 'true'
        |GROUP BY op,ak
      """.stripMargin).createOrReplaceTempView("new_visitor_table_op")
    //根据ak进行聚合，求出新用户的数量
    println("计算新用户的数量")
    spark.sql(
      """
        |SELECT COUNT(op) AS new_visitor_count, ak
        |FROM new_visitor_table_op
        |GROUP BY ak
      """.stripMargin).createOrReplaceTempView("new_visitor_table")


    /**
      *
      * 新增用户的留存人数
      */
    println("计算新用户的留存人数")
    spark.sql(
      """
        |SELECT COUNT(g.op) AS new_people_left, g.ak as ak
        |FROM
        | new_visitor_table_op g
        |JOIN
        | base_logs b
        |ON g.ak = b.ak AND g.op = b.op
        |GROUP BY g.ak
      """.stripMargin).createOrReplaceTempView("new_stay_table")

    /**
      * 活跃用户数
      */
    println("计算活跃用户数")
    spark.sql(
      """
        |SELECT op, ak
        |FROM logs_df
        |GROUP BY op, ak
      """.
        stripMargin).createOrReplaceTempView("old_visitor_table_op")

    spark.sql(
      """
        |SELECT count(op) as visitor_count, ak
        |FROM old_visitor_table_op
        |GROUP BY ak
      """.stripMargin).createOrReplaceTempView("old_visitor_table")

    /**
      * 活跃用户的留存人数
      */
    println("计算活跃用户的留存人数")

    spark.sql(
      """
        |SELECT COUNT(b.op) AS people_left, b.ak as ak
        |FROM
        | old_visitor_table_op g
        |JOIN
        | base_logs b
        |ON g.ak = b.ak AND g.op = b.op
        |GROUP BY b.ak
      """.stripMargin).createOrReplaceTempView("old_stay_table")

    println("聚合结果")

    val result_df = spark.sql(
      """
        |SELECT ovt.ak, nvt.new_visitor_count, ovt.visitor_count,
        |case when nvt.new_visitor_count=0 then 0 else cast(nst.new_people_left/nvt.new_visitor_count as float) end new_people_ratio,
        |case when ovt.visitor_count=0 then 0 else cast(ost.people_left/ovt.visitor_count as float) end active_people_ratio
        |FROM old_visitor_table AS ovt
        |LEFT JOIN new_visitor_table AS nvt ON ovt.ak = nvt.ak
        |LEFT JOIN old_stay_table AS ost ON ovt.ak = ost.ak
        |LEFT JOIN new_stay_table AS nst ON ovt.ak = nst.ak
      """.stripMargin).na.fill(0)
    println("写MySQL数据库")
    insert2db(result_df, String.valueOf(day_after), stat_day,"default", kafkaProducer)
  }

  /**
    * 结合渠道计算留存数据
    * @param spark
    * @param stat_day
    * @param day_after
    * @param dimension
    */
  private def userStaySource(spark: SparkSession, stat_day: String, day_after: Int,dimension:String, kafkaProducer: Broadcast[KafkaSink[String, String]]) = {
      /**
      * 新用户数
      */
    println(s"${dimension}-----新用户数")
    spark.sql(
      s"""
         |SELECT op, ak, ifo, ${dimension}
         |FROM
         |logs_df
      """.stripMargin).createOrReplaceTempView("logs_df_all")
    spark.sql(
      s"""
         |SELECT op, ak ,${dimension}
         |FROM logs_df_all
         |WHERE ifo = 'true'
         |GROUP BY op, ak, ${dimension}
      """.stripMargin).createOrReplaceTempView("new_visitor_table_op")

    spark.sql(
      s"""
         |SELECT COUNT(op) AS new_visitor_count, ak ,${dimension}
         |FROM new_visitor_table_op
         |GROUP BY ak, ${dimension}
      """.stripMargin).createOrReplaceTempView("new_visitor_table")

    /**
      *
      * 新增用户的留存人数
      */
    println(s"${dimension}---------新用户的留存人数")
    spark.sql(
      s"""
         |SELECT COUNT(g.op) AS new_people_left, g.ak as ak, g.${dimension} as ${dimension}
         |FROM
         |  new_visitor_table_op g
         |JOIN
         |  base_logs b
         |ON g.ak = b.ak AND g.op = b.op
         |GROUP BY g.ak, g.${dimension}
      """.stripMargin).
      createOrReplaceTempView("new_stay_table")

    /**
      * 活跃用户数
      */
    println(s"${dimension}-------活跃的用户数")
    spark.sql(

      s"""
         |SELECT op, ak, ${dimension}
         |FROM logs_df
         |GROUP BY op, ak, ${dimension}
      """.
        stripMargin).createOrReplaceTempView("old_visitor_table_op")
    spark.sql(
      s"""
         |SELECT COUNT(op) AS visitor_count, ak, ${dimension}
         |FROM old_visitor_table_op
         |GROUP BY ak, ${dimension}
      """.stripMargin).createOrReplaceTempView("old_visitor_table")

    /**
      * 活跃用户的留存人数
      */
    println(s"${dimension}---活跃用户的留存人数")
    spark.sql(
      s"""
         |SELECT COUNT(g.op) AS people_left, g.ak as ak, g.${dimension} as ${dimension}
         |FROM
         |  old_visitor_table_op g
         |JOIN
         |  base_logs b
         |WHERE g.ak = b.ak AND g.op = b.op
         |GROUP BY g.ak, g.${dimension}
      """.stripMargin).createOrReplaceTempView("old_stay_table")
    println(s"${dimension}----结果聚合")
    val result_df = spark.sql(
      s"""
         |SELECT ovt.ak, nvt.new_visitor_count, ovt.visitor_count,
         |case when nvt.new_visitor_count=0 then 0 else cast(nst.new_people_left/nvt.new_visitor_count as float) end new_people_ratio,
         |case when ovt.visitor_count=0 then 0 else cast(ost.people_left/ovt.visitor_count as float) end active_people_ratio,
         |ovt.${dimension}
         |FROM old_visitor_table AS ovt
         |LEFT JOIN new_visitor_table AS nvt ON ovt.ak = nvt.ak AND ovt.${dimension} = nvt.${dimension}
         |LEFT JOIN old_stay_table AS ost ON ovt.ak = ost.ak AND ovt.${dimension} = ost.${dimension}
         |LEFT JOIN new_stay_table AS nst ON ovt.ak = nst.ak AND ovt.${dimension} = nst.${dimension}
      """.stripMargin).na.fill(0)

    insert2db(result_df, String.valueOf(day_after), stat_day,dimension, kafkaProducer)
  }

  /**
    * 批量入库
    * @param rs 结果集
    * @param day_after  几日后留存
    * @param stat_day 计算日期
    * @param dimension 渠道, 场景值，外链）
    */
  private def insert2db(rs: DataFrame,day_after:String,stat_day:String,dimension:String, kafkaProducer: Broadcast[KafkaSink[String, String]]) = {
    rs.foreachPartition((rows: Iterator[Row]) => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
        rows.foreach(r => {
            var sql = ""
            val ak = r(0)
            val day = stat_day
            var diff = day_after
            val people_left = r(2) //活跃用户
            val new_people_left = r(1) //新增用户
            var active_people_ratio = r(4) //活跃用户留存比
            var new_people_ratio = r(3) //新增用户留存比
            if (day_after == "0") { //如果是昨日，则不插入留存比
              active_people_ratio = "0"
              new_people_ratio = "0"
            }

            var link_key = "default"
            var media_id = "default"
            var position_id = "default"
            if (dimension == "default") {
              sql = s"insert into ald_stay_logs_link (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio,link_key)" +
                s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}','${link_key}') " +
                s"ON DUPLICATE KEY UPDATE " +
                s"new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}',link_key='${link_key}'"
            } else if (dimension == "wsr_query_ald_link_key") {
              link_key = r(5).toString
              sql = s"insert into ald_stay_logs_link (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio,link_key)" +
                s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}','${link_key}') " +
                s"ON DUPLICATE KEY UPDATE " +
                s"new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}',link_key='${link_key}'"
            } else if (dimension == "wsr_query_ald_media_id") {
              media_id = r(5).toString
              sql = s"insert into ald_stay_logs_media (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio,media_id)" +
                s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}','${media_id}') " +
                s"ON DUPLICATE KEY UPDATE " +
                s"new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}',media_id='${media_id}'"
            } else if (dimension == "ag_ald_position_id") {
              position_id = r(5).toString
              sql = s"insert into ald_stay_logs_position (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio,position_id)" +
                s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}','${position_id}') " +
                s"ON DUPLICATE KEY UPDATE " +
                s"new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}',position_id='${position_id}'"
            }
          sendToKafka(kafka, sql)
        })
    })
  }

}
