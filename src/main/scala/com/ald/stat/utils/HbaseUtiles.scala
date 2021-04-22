package com.ald.stat.utils

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}


object HbaseUtiles {

  val configuration = new Configuration
//  configuration.set("hbase.zookeeper.quorum","10.0.100.13,10.0.100.8,10.0.100.14")
    configuration.set("hbase.zookeeper.quorum","10.0.80.13")//196的zookeeper信息
  configuration.set("hbase.zookeeper.property.clientPort","2181")
  val connection = ConnectionFactory.createConnection(configuration)
  val admin = connection.getAdmin
//  val table = connection.getTable(TableName.valueOf("STAT.ALDSTAT_LINK_AUTH_OPENID"))
  val table = connection.getTable(TableName.valueOf(ConfigUtils.getProperty("hbase.table.name")))
  val family = "common"
//  val column = "online"
//  val value = "1"

  //向hbase表中插入数据
  //put 'STAT.ALDSTAT_LINK_AUTH_OPENID','openId + ak','online:status','0'
  //如果没有这个key，那么新增授权+1，如果有，就get 'STAT.ALDSTAT_LINK_AUTH_OPENID','openid + ak'
  def insertTable(rowkey: String, hbaseColumn: String, currentTime: Long) = {
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    //添加列簇名,字段名,字段值value
    puts.addColumn(family.getBytes(), hbaseColumn.getBytes(), currentTime.toString.getBytes())
    //把数据插入到tbale中
    table.put(puts)
//    println("insert successful!!")
  }

  //get 'STAT.ALDSTAT_LINK_AUTH_OPENID','openid + ak'
  /**
    *
    * @param rowKey
    * @param currentTime  列值
    * @param hbaseColumn  列名
    * @return
    */
  def getDataFromHTable (rowKey: String, currentTime: Long, hbaseColumn: String) : (String, String)= {
    val gets = new Get(rowKey.getBytes())
    gets.addColumn(family.getBytes(), hbaseColumn.getBytes())
    val result = table.get(gets)
    if(result.isEmpty){
      insertTable(rowKey, hbaseColumn, currentTime)
//      println("storing..........................")
      return ("notStored", "notStored")
    }else{
      val insertTime = result.listCells().get(0).getTimestamp
      val cell = new String(result.getValue(family.getBytes(), hbaseColumn.getBytes()))
//      println("openIdStatus ============ " + cell)
      (cell, insertTime.toString)
    }
  }

  def getDataFromHTableWithName (rowKey: String, currentTime: Long, hbaseColumn: String, tableName: String) : (String, String)= {
    val gets = new Get(rowKey.getBytes())
    gets.addColumn(family.getBytes(), hbaseColumn.getBytes())
    val table = connection.getTable(TableName.valueOf(tableName))
    val result = table.get(gets)
    if(result.isEmpty){
      insertTableWithName(rowKey, hbaseColumn, currentTime, tableName)
      //      println("storing..........................")
      return ("notStored", "notStored")
    }else{
      val insertTime = result.listCells().get(0).getTimestamp
      val cell = new String(result.getValue(family.getBytes(), hbaseColumn.getBytes()))
      //      println("openIdStatus ============ " + cell)
      (cell, insertTime.toString)
    }
  }
  def insertTableWithName(rowkey: String, hbaseColumn: String, currentTime: Long, tableName: String) = {
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    val table = connection.getTable(TableName.valueOf(tableName))
    //添加列簇名,字段名,字段值value
    puts.addColumn(family.getBytes(), hbaseColumn.getBytes(), currentTime.toString.getBytes())
    //把数据插入到tbale中
    table.put(puts)
    //    println("insert successful!!")
  }

  def getDataFromHTable2 (rowKey: String, currentTime: Long, hbaseColumn: String) : (String, String)= {
    val gets = new Get(rowKey.getBytes())
    gets.addColumn(family.getBytes(), hbaseColumn.getBytes())
    val result = table.get(gets)
    if(result.isEmpty){
//      insertTable(rowKey, hbaseColumn, currentTime)
      //      println("storing..........................")
      return ("notStored", "notStored")
    }else{
      val insertTime = result.listCells().get(0).getTimestamp
      val cell = new String(result.getValue(family.getBytes(), hbaseColumn.getBytes()))
      //      println("openIdStatus ============ " + cell)
      (cell, insertTime.toString)
    }
  }

  // 关闭 connection 连接
  def close()={
    if (connection!=null){
      try{
        connection.close()
        println("Close success!")
      }catch{
        case e:IOException => println("Close failure!")
      }
    }
  }
}