package com.ald.stat.etl.factTable

import org.apache.spark.sql.SparkSession

/**
  * Created by zhaofw on 2018/9/5.
  * 构建事实表
  * 通过对原始数据的初步聚合，得到满足用户分群使用的事实表
  */
object BuildFactTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BuildFactTable").master("local[2]").getOrCreate()
    //val path = "C:\\Users\\admin\\Desktop\\part-00015-909bdc07-ddf8-4b05-874f-048abd281109-c000.snappy.parquet"
    val path = "D:\\LogData\\0828\\082011.parquet"
    val logs = spark.read.option("mergeSchema", "true").parquet(path)
    logs.createOrReplaceTempView("logs")
    logs.filter("ifo='true'").show(100)
    logs.printSchema()

    spark.stop()

  }


}
