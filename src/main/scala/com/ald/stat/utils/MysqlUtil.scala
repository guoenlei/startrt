package com.ald.stat.utils

import java.sql.{Connection, DriverManager}
import java.util

import com.ald.stat.kafka.appletJob.Write2MysqlSplit.logger

import scala.collection.mutable

/**
 * 分库存表，根据连接名，判断map中是否有该连接，返回连接
 * 采用单例模式，一个应用只有一个数据库连接
 */
object MysqlUtil {
  //  val coonMap = new mutable.HashMap[String,Connection]()
  //  val coonMap = new util.HashMap[String,Connection]()
  val prefix = "default"
  val qqPrefix = "qq_default"

  def getOtherConnection(connName: String, url: String, user: String, pwd: String): Connection = {
    //    if(coonMap.get(connName) == null){
    try {
      Class.forName(ConfigUtils.getProperty("database.driver"))
      val conn = DriverManager getConnection(url, user, pwd)
      //        coonMap.put(connName, conn)
      //        return coonMap.get(connName)
      return conn
    } catch {
      case t: Throwable => {
        logger.error("Create a connection error!!!!!! other connection---------------" + connName, t)
      }
      //      }
      //    }else{
      //      return  coonMap.get(connName)
      //    }
    }
    return null
  }

  /**
   * 微信默认库
   *
   * @return
   */
  def getConnection(): Connection = {
    //    if(coonMap.get(prefix) == null){
    try {
      Class.forName(ConfigUtils.getProperty("database.driver"))
      val conn = DriverManager getConnection(ConfigUtils.getProperty("database.url"), ConfigUtils.getProperty("database.user"), ConfigUtils.getProperty("database.password"))
      //        coonMap.put(prefix, conn)
      //        return coonMap.get(prefix)
      return conn
    } catch {
      case t: Throwable => {
        logger.error("Create a connection error!!!!!! other connection---------------" + prefix, t)
      }
      //      }
      //    }else{
      //      return  coonMap.get(prefix)
      //    }
    }
    return null
  }

  /**
   * QQ默认库
   *
   * @return
   */
  def getQQConnection(): Connection = {
    //    if(coonMap.get(qqPrefix) == null){
    try {
      Class.forName(ConfigUtils.getProperty("database.driver"))
      val conn = DriverManager getConnection(ConfigUtils.getProperty("qq.database.url"), ConfigUtils.getProperty("qq.database.user"), ConfigUtils.getProperty("qq.database.password"))
      //        coonMap.put(qqPrefix, conn)
      //        return coonMap.get(qqPrefix)
      return conn
    } catch {
      case t: Throwable => {
        logger.error("Create a connection error!!!!!! other connection---------------" + qqPrefix, t)
      }

      //    }else{
      //      return  coonMap.get(qqPrefix)
      //    }

    }
    return null
  }
}