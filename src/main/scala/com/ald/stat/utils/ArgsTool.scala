package com.ald.stat.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ArgsTool {
  var day: String =  ComputeTimeUtils.getUdfDate(-1, "yyyy-MM-dd")
  var hour = ""
  var ak = ""
  var vak = ""
  var partition = 0
  var du = ""

  /**
    * 初始化传递过来的参数
    *
    * @param args main方法的参数
    */
  def analysisArgs(args: Array[String]): Unit = {
    args.foreach {
      case "-d" => day = args(args.indexOf("-d") + 1)
      case "-h" => hour = args(args.indexOf("-h") + 1)
      case "-ak" => ak = args(args.indexOf("-ak") + 1)
      case "-vak" => vak = args(args.indexOf("-vak") + 1)
      case "-np" => partition = args(args.indexOf("-np") + 1).toInt
      case "-du" => du = args(args.indexOf("-du") + 1)
      case any => println(s"接收到的参数：$any")
    }
    println(s"initialize args：day=$day,hour=$hour,partition=$partition,du=$du,ak=$ak,vak=$vak")
  }

  def getLogs(spark: SparkSession, logPath: String): RDD[String] = {
      println(s"获取$day 数据")
      getDailyDataFrame(spark, logPath)

  }

  def getLogsDF(spark: SparkSession, path: String): DataFrame= {
    println(s"获取$day 数据")
    val dayDir = day.split("-").mkString("")
    var logPath = ""
    logPath = s"$path/$dayDir/*$hour/*"
    var logs = spark.read.parquet(logPath)
    if (partition > 0) logs.repartition(partition) else logs
  }

  /**
    *
    * @param path hdfs路径 hdfs://xxxx/ald_log_parquet
    * @return 返回每日的数据（DataFrame）
    */
  def getDailyDataFrame(spark: SparkSession, path: String): RDD[String] = {
    //2017-12-20 -> 20171220
    val dayDir = day.split("-").mkString("")
    println(s"$day 转换为 $dayDir")
    var logPath = ""
    logPath = s"$path/$dayDir/*$hour/*"
    println(s"获取${logPath}的数据")
    var logs = spark.read.parquet(logPath).filter("wsr_query_ald_link_key != 'null'").toJSON.rdd
//    var logs = spark.read.option("mergeSchema", "true").parquet(logPath)

//    if (ak != "") {
//      val aks = ak.split(",").mkString("'", "','", "'")
//      println(s"过滤出${aks}的数据")
//      logs = logs.filter(s"ak in ($aks)")
//      //logs.show(false)
//    } else if (vak != "") {
//      val vaks = vak.split(",").mkString("'", "','", "'")
//      println(s"过滤掉${vaks}的数据")
//      logs = logs.filter(s"ak not in ($vaks)")
//      //logs.show(false)
//    }
    if (partition > 0) logs.repartition(partition) else logs

  }

  def getDailyDataFrameWithFilter(spark: SparkSession, path: String): RDD[String] = {
    //2017-12-20 -> 20171220
    val dayDir = day.split("-").mkString("")
    println(s"$day 转换为 $dayDir")
    var logPath = ""
    logPath = s"$path/$dayDir/*$hour/*"
    println(s"获取${logPath}的数据")
    var logs = spark.read.parquet(logPath).filter(s"v >= '7.3.0'").select("ak","at","et","dr","pp","ifo","img","wsr_query_ald_link_key","wsr_query_ald_media_id","scene","op").toJSON.rdd
    if (partition > 0) logs.repartition(partition) else logs

  }

  /**
    * 读取cos上的openid
    * @param spark
    * @param path
    * @return
    */
  def getOpenidRDDFromCos(spark: SparkSession, path: String): RDD[(String, String)] = {
    //2017-12-20 -> 20171220
    val dayDir = day.split("-").mkString("")
    println(s"$day 转换为 $dayDir")
    var logPath = ""
    logPath = s"$path/$dayDir/*"
    println(s"获取${logPath}的数据")

    val logs = spark.sparkContext.wholeTextFiles(logPath)
    if (partition > 0) logs.repartition(partition) else logs
  }

  /**
    * 弹性mr日志目录
    * cosn://aldwxlogbackup/log_parquet/zzcg-etl-VM-0-224-ubuntu2017120313/
    * cosn://aldwxlogbackup/log_parquet/星2017120313/
    */
  def getTencentDailyDataFrame(spark: SparkSession, path: String): DataFrame = {
    //2017-12-20 -> 20171220
    val dayDir = day.split("-").mkString("")
    println(s"$day 转换为 $dayDir")
    val logPath = s"$path/*$dayDir$hour*/*"
    println(s"获取${logPath}的数据")
    var logs = spark.read.option("mergeSchema", "true").parquet(logPath)

    if (ak != "") {
      val aks = ak.split(",").mkString("'", "','", "'")
      println(s"过滤出${aks}的数据")
      logs = logs.filter(s"ak in ($aks)")
      //logs.show(false)
    } else if (vak != "") {
      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
      //logs.show(false)
    }
    if (partition > 0) logs.repartition(partition) else logs

  }

//  /**
//    *
//    * @param path    hdfs路径 hdfs://xxxx/ald_log_parquet
//    * @param numDays 7或30
//    * @return 返回7日或30日的数据（DataFrame）
//    */
//  def getSevenOrThirtyDF(spark: SparkContext, path: String, numDays: String): DataFrame = {
//    val listDirs = ComputeTimeUtils.getUdfDaysDirs(path, day, numDays.toInt)
//    println(s"读取${numDays}天数据")
//    var logs = spark.read.option("mergeSchema", "true").parquet(listDirs: _*)
//
//    if (ak != "") {
//      val aks = ak.split(",").mkString("'", "','", "'")
//      println(s"过滤出${numDays}天${aks}的数据")
//      logs = logs.filter(s"ak in ($aks)")
//    } else if (vak != "") {
//      val vaks = vak.split(",").mkString("'", "','", "'")
//      println(s"过滤掉${numDays}天${vaks}的数据")
//      logs = logs.filter(s"ak not in ($vaks)")
//    }
//    if (partition > 0) logs.repartition(partition) else logs
//  }

//  /**
//    * 获取某一天的数据
//    * @param spark
//    * @param path
//    * @param day
//    * @return
//    */
//  def getDFOfDay(spark: SparkSession, path: String, day: String): DataFrame = {
//    val dayDir = day.split("-").mkString("")
//    var logPath = ""
//    logPath = s"$path/$dayDir/*$hour/*"
//    println(s"获取${logPath}的数据")
//    var logs = spark.read.option("mergeSchema", "true").parquet(logPath)
//
//    if (partition > 0) logs.repartition(partition) else logs
//  }

  def getDFOfDay(spark: SparkSession, path: String, day: String,customSchema:StructType): DataFrame = {
    val dayDir = day.split("-").mkString("")
    var logPath = ""
    logPath = s"$path/$dayDir/*$hour/*"
    println(s"获取${logPath}的数据")
    var logs = spark.read.schema(customSchema).parquet(logPath).filter("v >= '7.3.0'")

    if (partition > 0) logs.repartition(partition) else logs
  }


  /**
    * 弹性mr日志目录，获取7天或30天数据
    * cosn://aldwxlogbackup/log_parquet/zzcg-etl-VM-0-224-ubuntu2017120313/
    * cosn://aldwxlogbackup/log_parquet/星2017120313/
    */
  def getTencentSevenOrThirtyDF(args: Array[String], spark: SparkSession, numDays: String): DataFrame = {
    analysisArgs(args)
    val listDirs = ComputeTimeUtils.getTencentDirs(day, numDays.toInt)
    println(s"读取Tencent${numDays}天数据")
    var logs = spark.read.option("mergeSchema", "true").parquet(listDirs: _*)

    if (ak != "") {
      val aks = ak.split(",").mkString("'", "','", "'")
      println(s"过滤出${numDays}天${aks}的数据")
      logs = logs.filter(s"ak in ($aks)")
    } else if (vak != "") {
      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${numDays}天${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
    }
    if (partition > 0) logs.repartition(partition) else logs
  }

//  /**
//    * 获取erm指定天数的数据集合的DataFrame
//    * @param args 参数
//    * @param spark 。。。
//    * @param daysArr 存要获取的天数的数组
//    * @return DF
//    */
//  def getSpecifyTencentDateDF(args: Array[String], spark: SparkSession, daysArr: Array[Int]): DataFrame = {
//    analysisArgs(args)
//    val listDirs = ComputeTimeUtils.getSpecifyTencentDirs(day,daysArr)
//    println(s"读取Tencent${daysArr}天数据")
//    var logs = spark.read.option("mergeSchema", "true").parquet(listDirs: _*)
//
//    if (ak != "") {
//      val aks = ak.split(",").mkString("'", "','", "'")
//      println(s"过滤出${daysArr}天${aks}的数据")
//      logs = logs.filter(s"ak in ($aks)")
//    } else if (vak != "") {
//      val vaks = vak.split(",").mkString("'", "','", "'")
//      println(s"过滤掉${daysArr}天${vaks}的数据")
//      logs = logs.filter(s"ak not in ($vaks)")
//    }
//    if (partition > 0) logs.repartition(partition) else logs
//  }
}
