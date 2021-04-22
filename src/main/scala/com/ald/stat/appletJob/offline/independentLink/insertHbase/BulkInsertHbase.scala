//package com.ald.stat.appletJob.offline.independentLink.insertHbase
//
//
//import com.ald.stat.cache.{AbstractRedisCache, CacheClientFactory, CacheRedisFactory, ClientRedisCache}
//import com.ald.stat.utils.DBUtils.use
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, sql}
//import org.apache.spark.sql.SparkSession
//import org.slf4j.LoggerFactory
//import redis.clients.jedis.exceptions.JedisConnectionException
//
//import scala.collection.mutable.ArrayBuffer
//
//object BulkInsertHbase {
//  val logger = LoggerFactory.getLogger(this.getClass.getName)
//  val prefix = "insertHbase"
//  def main(args: Array[String]): Unit = {
//    val newUserHbaseColumn = "newuser"
////    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID_TEST"//测试
//    val newUserTableName = "STAT.ALDSTAT_ADVERTISE_OPENID"
//
//    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
//
//    val ssc = SparkSession.builder()
//      .appName(this.getClass.getName)
//      .getOrCreate()
//
//    val redisCache = CacheRedisFactory.getInstances(prefix).asInstanceOf[AbstractRedisCache]
//    val resource = redisCache.getResource
//    val pattern = s"*.openid.set" //需要删除的key，包括pv和session相关的key
//    val akAndOpenid = new ArrayBuffer[(String,String)]
//    println(pattern)
//    try {
//      use(redisCache.getResource) {
//        resource => {
//          val iter = resource.getAllShards.iterator()
//          while (iter.hasNext) {
//            val jedis = iter.next()
//            val kIter = jedis.keys(pattern).iterator()
//            while (kIter.hasNext) {
//              val key = kIter.next()
//              val ak = key.split("\\.")(0)
//              val v = jedis.get(key)
//              val openidArray = v.split(",")
//              for(openid <- openidArray){
//                akAndOpenid.+=((ak,openid))
//              }
//              println(s"$key")
//              jedis.del(key)
//            }
//          }
//        }
//      }
//    } catch {
//      case jce: JedisConnectionException => jce.printStackTrace()
//    } finally {
//      if (resource != null) resource.close()
//      if (redisCache != null) redisCache.close()
//    }
//    val finalRDD = ssc.sparkContext.parallelize(akAndOpenid)
//    //默认批量出入的老用户openid列值存储事件为：2019-06-01
//    val dayTime = "1559318400000"
//    bulkInsertHbase2(finalRDD, newUserTableName, newUserHbaseColumn, dayTime)
//    ssc.close()
//  }
//
//  /**
//    * 批量写入hbase
//    * @param finalRDD
//    * @param tableName
//    * @param hbaseColumn
//    * @param dayTime
//    */
//  def bulkInsertHbase2(finalRDD: RDD[(String, String)], tableName: String, hbaseColumn:String, dayTime:String): Unit = {
//    val configuration = new Configuration
//    configuration.set("hbase.zookeeper.quorum", "10.0.100.13,10.0.100.8,10.0.100.14")
//    configuration.set("hbase.zookeeper.property.clientPort", "2181")
//
//    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
//    val jobConf = Job.getInstance(configuration)
//    //val jobConf = new Job()
//    jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
//    jobConf.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    //val currentTime = new Date().getTime
//    val a = finalRDD.mapPartitions(iter => {
//      iter.map(r => {
//        val ak = r._1
//        val op = r._2
//        val rowKey = op.reverse + ":" + ak
//        val put = new Put(Bytes.toBytes(rowKey))
//        put.addColumn(Bytes.toBytes("common"), Bytes.toBytes(hbaseColumn), dayTime.getBytes())
//        (new ImmutableBytesWritable, put)
//      })
//    })
//    a.saveAsNewAPIHadoopDataset(jobConf.getConfiguration) //存入HBase
//  }
//}
