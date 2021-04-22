package com.ald.stat.appletJob.task

import java.util.Date

import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, KafkaSink, SDKVersionUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object DailyVersionTask extends TaskTrait {

  /**
   * 更新SDK版本
   *
   * @param logRecordRdd
   */
  def versionStat(logRecordRdd: RDD[(String,String)], kafkaProducer: Broadcast[KafkaSink[String, String]]): Unit = {

    val finalRDD = logRecordRdd.distinct()

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val day = ComputeTimeUtils.getDateStr(new Date())
      par.foreach(row => {
        val ak = row._1
        val v = row._2
        // 符合 num.n.n 格式才进入切分
        if (SDKVersionUtils.properSDK(v)) {
          val splits = v.split("\\.")
          //第三方标识，1代表第三方，版本号位数是单数的为第三方
          var is_third = 0
          if (splits.length == 3) {
            // QQ版本号格式不同，qqMini：8.0.0，wxMini：7.3.0。qqGame，wxGAME:2.0.0，后期升3.0.0
            try {
              if (splits(2).toInt % 2 != 0) {
                is_third = 1
              }
            } catch {
              case e: NumberFormatException => println(splits)
            }
          }

          val sqlInsertOrUpdate =
            s"""
               |insert into ald_sdk_version_apps
               |(
               |    app_key,
               |    day,
               |    is_third,
               |    version
               |)
               |values(
               |    "$ak",
               |    "$day",
               |    "$is_third",
               |    "$v"
               |)
               |on DUPLICATE KEY UPDATE
               |version="$v",
               |is_third="$is_third"
                          """.stripMargin

          //数据进入kafka

          sendToKafkaWithAK(kafka, sqlInsertOrUpdate, ak)


        }
      })
    })
  }

}
