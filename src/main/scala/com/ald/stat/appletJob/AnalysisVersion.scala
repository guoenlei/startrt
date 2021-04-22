package com.ald.stat.appletJob

import java.util.Properties

import com.ald.stat.appletJob.task.DailyVersionTask
import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
  * 更新每个SDK的最新版本到mysql中
  */
object AnalysisVersion extends AbstractBaseJob {

  val prefix = "analysisVersion"

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //                  .set("spark.executor.cores", "2")
      //      .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5)) //设置为5秒一个批次

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")

    val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram

    /**业务操作*/
    stream.foreachRDD(rdd => {
      val logRecordRdd = rdd.mapPartitions(par => {
//        val recordsRdd = ArrayBuffer[LogRecord]()
        val recordsRdd = new mutable.HashSet[(String,String)]
        par.foreach(line => {
          val logRecord = LogRecord.line2Bean(line.value())
          if (logRecord != null &&
            StringUtils.isNotBlank(logRecord.ak) &&
            StringUtils.isNotBlank(logRecord.v)
          ) {
            recordsRdd.add((logRecord.ak,logRecord.v))
          }
        })
        recordsRdd.iterator
      })

      DailyVersionTask.versionStat(logRecordRdd, kafkaProducer)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
