package com.ald.stat.utils

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._

/**
  * Created with IntelliJ IDEA.
  * User: @ziyu  freedomziyua@gmail.com
  * Date: 2019-09-06
  * Time: 18:29
  * Description: 
  */
object HbaseUtilesNew {

  val configuration = new Configuration
  configuration.set("hbase.zookeeper.quorum", "10.0.100.13,10.0.100.8,10.0.100.14")
  //  configuration.set("hbase.zookeeper.quorum","10.0.80.13")//196的zookeeper信息
  configuration.set("hbase.zookeeper.property.clientPort", "2181")

  val family = "common"

  def insertTable(rowkey: String, hbaseColumn: String, currentTime: Long, connection: Tuple2[Connection, Table]) = {
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    //添加列簇名,字段名,字段值value
    puts.addColumn(family.getBytes(), hbaseColumn.getBytes(), currentTime.toString.getBytes())
    //把数据插入到tbale中
    connection._2.put(puts)
    //    println("insert successful!!")
  }

  //get 'STAT.ALDSTAT_LINK_AUTH_OPENID','openid + ak'
  /**
    *
    * @param rowKey
    * @param currentTime 列值
    * @param hbaseColumn 列名
    * @return
    */
  def getDataFromHTable(rowKey: String, currentTime: Long, hbaseColumn: String, connection: Tuple2[Connection, Table]): (String, String) = {
    val gets = new Get(rowKey.getBytes())
    gets.addColumn(family.getBytes(), hbaseColumn.getBytes())
    val result = connection._2.get(gets)
    if (result.isEmpty) {
      insertTable(rowKey, hbaseColumn, currentTime, connection)
      //      println("storing..........................")
      return ("notStored", "notStored")
    } else {
      val insertTime = result.listCells().get(0).getTimestamp
      val cell = new String(result.getValue(family.getBytes(), hbaseColumn.getBytes()))
      //      println("openIdStatus ============ " + cell)
      (cell, insertTime.toString)
    }
  }

  def getDataFromHTableWithName(rowKey: String, currentTime: Long, hbaseColumn: String, tableName: String, connection: Tuple2[Connection, Table]): (String, String) = {
    val gets = new Get(rowKey.getBytes())
    gets.addColumn(family.getBytes(), hbaseColumn.getBytes())
    val table = connection._2
    val result = table.get(gets)
    if (result.isEmpty) {
      insertTableWithName(rowKey, hbaseColumn, currentTime, tableName, connection)
      //      println("storing..........................")
      return ("notStored", "notStored")
    } else {
      val insertTime = result.listCells().get(0).getTimestamp
      val cell = new String(result.getValue(family.getBytes(), hbaseColumn.getBytes()))
      //      println("openIdStatus ============ " + cell)
      (cell, insertTime.toString)
    }
  }

  def insertTableWithName(rowkey: String, hbaseColumn: String, currentTime: Long, tableName: String, connection: Tuple2[Connection, Table]) = {
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    val table = connection._2
    //添加列簇名,字段名,字段值value
    puts.addColumn(family.getBytes(), hbaseColumn.getBytes(), currentTime.toString.getBytes())
    //把数据插入到tbale中
    table.put(puts)
    //    println("insert successful!!")
  }

  def getDataFromHTable2(rowKey: String, currentTime: Long, hbaseColumn: String, connection: Tuple2[Connection, Table]): (String, String) = {
    val gets = new Get(rowKey.getBytes())
    gets.addColumn(family.getBytes(), hbaseColumn.getBytes())
    val result = connection._2.get(gets)
    if (result.isEmpty) {
      //      insertTable(rowKey, hbaseColumn, currentTime)
      //      println("storing..........................")
      return ("notStored", "notStored")
    } else {
      val insertTime = result.listCells().get(0).getTimestamp
      val cell = new String(result.getValue(family.getBytes(), hbaseColumn.getBytes()))
      //      println("openIdStatus ============ " + cell)
      (cell, insertTime.toString)
    }
  }


  // 打开Hbase connection 连接
  def getHbaseConn(): Tuple2[Connection, Table] = {
    val connection = ConnectionFactory.createConnection(configuration)
    val admin = connection.getAdmin
    //  val table = connection.getTable(TableName.valueOf("STAT.ALDSTAT_LINK_AUTH_OPENID"))
    val table = connection.getTable(TableName.valueOf(ConfigUtils.getProperty("hbase.table.name")))
    println("Open success!")
    (connection, table)
  }


  // 关闭 connection 连接
  def close(connection: Tuple2[Connection, Table]) = {
    if (null != connection._1 && null != connection) {
      try {
        connection._1.close()
        println("Close success!")
      } catch {
        case e: IOException => println("Close failure!")
      }
    }
  }

}
