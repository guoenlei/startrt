package com.ald.stat.appletJob.offline.independentLink.insertHbase

import java.net.URI

import com.ald.stat.utils.ArgsTool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object BulkInsertHbaseFromCos {
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val newUserHbaseColumn = "newuser"
//    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID_TEST" //测试
        val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID"

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    val ssc = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()
    ArgsTool.analysisArgs(args)
    val day = ArgsTool.day
    val path = s"cosn://aldpicsh-1252823355/openid/$day"
    val fileSystem = FileSystem.newInstance(URI.create(path), new Configuration())
    if(fileSystem.exists(new Path(path))){
      val pathAndOpenid = ArgsTool.getOpenidRDDFromCos(ssc, "cosn://aldpicsh-1252823355/openid/")
      val finalRDD = pathAndOpenid.mapPartitions(par => {
        val akAndOpenid = new ArrayBuffer[(String, String)]
        par.foreach(line => {
          val suffix = line._1.split("\\.")(1)
          if (suffix == "csv") {
            val lastIndex = line._1.lastIndexOf("/") + 1
            val splits = line._1.substring(lastIndex).split("_")
            renameFile(line._1, ".csv")
            val ak = splits(0)
            val openidList = line._2.split("\r\n") //unix下\n就行
            for (openid <- openidList) {
              //openid满足28位是正常的
              if (openid.length == 28) {
                akAndOpenid.+=((ak, openid))
              }
            }
          }
        })
        akAndOpenid.iterator
      })
      //默认批量出入的老用户openid列值存储事件为：2019-06-01
      val dayTime = "1559318400000"
      bulkInsertHbase2(finalRDD, newUserTableName, newUserHbaseColumn, dayTime)
    }
    ssc.close()
  }

  /**
    * 批量写入hbase
    *
    * @param finalRDD
    * @param tableName
    * @param hbaseColumn
    * @param dayTime
    */
  def bulkInsertHbase2(finalRDD: RDD[(String, String)], tableName: String, hbaseColumn: String, dayTime: String): Unit = {
    val configuration = new Configuration
    configuration.set("hbase.zookeeper.quorum", "10.0.100.13,10.0.100.8,10.0.100.14")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
    val jobConf = Job.getInstance(configuration)
    //val jobConf = new Job()
    jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    jobConf.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //val currentTime = new Date().getTime
    val a = finalRDD.mapPartitions(iter => {
      iter.map(r => {
        val ak = r._1
        val op = r._2
        val rowKey = op.reverse + ":" + ak
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("common"), Bytes.toBytes(hbaseColumn), dayTime.getBytes())
        (new ImmutableBytesWritable, put)
      })
    })
    //存入HBase
    a.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
  }

  /**
    * 将cos上的.csv文件后缀改为.txt
    *
    * @param path
    * @param sourceFile
    */
  def renameFile(path: String, sourceFile: String): Unit = {
    val fileSystem = FileSystem.newInstance(URI.create(path), new Configuration())
    val fsArr: Array[FileStatus] = fileSystem.listStatus(new Path(path))
    val paths: Array[Path] = FileUtil.stat2Paths(fsArr)
    val arr = paths.filter(fileSystem.getFileStatus(_).isFile()).map(_.toString)
    println("===========原文件如下==================")
    arr.foreach(println)

    println("===========下面执行重命名函数===========")
    for (i <- 0.until(arr.length)) {
      val beforeFilePath = arr(i)
      val lastIndex = beforeFilePath.lastIndexOf("/") + 1
      val beforeFileName = beforeFilePath.substring(lastIndex)
      if (beforeFileName.contains(sourceFile)) {
        //        val num = beforeFilePath.substring(lastIndex + 6, lastIndex + 10).toInt
        val newFileName = beforeFileName.split("\\.")(0)
        val afterFilePath = beforeFilePath.substring(0, lastIndex) + s"$newFileName.txt"
        fileSystem.rename(new Path(beforeFilePath), new Path(afterFilePath))
        println(s"$beforeFileName 被重命名为 $afterFilePath ")
      } else {
        println(s"$beforeFileName 这个不是我想要的重命名的文件")
      }
    }
  }
}
