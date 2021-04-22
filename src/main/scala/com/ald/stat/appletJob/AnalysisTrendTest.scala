package com.ald.stat.appletJob

import java.util.Properties

import com.ald.stat.kafka.appletJob.KafkaConsume
import com.ald.stat.utils.{ConfigUtils, KafkaSink}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object AnalysisTrendTest extends AbstractBaseJob {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    val ssc:StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    // 定义kafka生产者配置，并把该配置广播出去
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

    //配置kafka相关参数
    val kafkaParams = Map[String,Object](
      Tuple2("a","b")



    )
    // kafka direct方式


  }

}
