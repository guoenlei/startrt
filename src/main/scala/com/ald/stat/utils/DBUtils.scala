package com.ald.stat.utils

import java.sql.{Connection, DriverManager, SQLException}
import java.util

import com.ald.stat.kafka.appletJob.Write2MysqlSplit.logger
import com.alibaba.fastjson.JSONException
import org.apache.phoenix.jdbc.PhoenixResultSet
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object DBUtils {
  //  lazy val datasource = getDatasource()
  //  lazy Connection
  val batchSize = 1000

  def doExecute(sql: String): Int = {
    use(getConnection) { connection => {
      val rint = use(connection.createStatement()) { stmt =>
        stmt.executeUpdate(sql)

      }
      rint
    }

    }
  }

  def doBatchExecute(sqls: List[String]): Int = use(getConnection) {
    connection => {
      use(connection.createStatement()) { stmt => {
        var count: Int = 0
        sqls.foreach(sql => {
          stmt.addBatch(sql)
          if (count % batchSize == 0) stmt.executeBatch
        })
        count += 1
        stmt.executeBatch
        count
      }
      }
    }
  }

  def getConnection(): Connection = {
    Class.forName(ConfigUtils.getProperty("database.driver"))
    val conn = DriverManager getConnection(ConfigUtils.getProperty(
      "database.url"
    ), ConfigUtils.getProperty("database.user"), ConfigUtils.getProperty(
      "database.password"
    ))
    conn
  }

  def getSplitConnection(): Connection = {
    Class.forName(ConfigUtils.getProperty("database.driver"))
    val conn = DriverManager getConnection(ConfigUtils.getProperty(
      "split.database.url"
    ), ConfigUtils.getProperty("split.database.user"), ConfigUtils.getProperty(
      "split.database.password"
    ))
    conn
  }


  def getSplitDBTuple() = {
    val coonMap = new mutable.HashMap[String, Tuple6[String,
      String,
      String,
      String,
      String,
      String]]
    use(getSplitConnection()) { conn =>
      use(conn.createStatement()) { statement =>
        val rs = statement.executeQuery(
          """
            |select platform,app_key,dbname,dbip,port,dbuser,dbpassword
            |from ald_db_split
            """.stripMargin
        )
        while (rs.next()) {
          val app_key = rs.getString("app_key")
          //            val connStr = rs.getString("conn_name") + ":" + rs.getString(3) + ":" + rs.getString(4) + ":" + rs.getString(5) + ":" + rs.getString(6) + ":" + rs.getString(7)
          //              if (app_key != null && connStr != null) {
          coonMap.put(
            app_key,
            (
              rs.getString("platform"),
              rs.getString("dbname"),
              rs.getString("dbip"),
              rs.getString("port"),
              rs.getString("dbuser"),
              rs.getString("dbpassword")
            )
          )
          //            }
        }
      }
    }
    coonMap
  }

  def readQRMapByStateMap(conn: Connection, sql: String, dictionaryMap: util.HashMap[String, String]) = {
    try {
      use(conn.createStatement()) {
        defaultStatement => {
          val rs = defaultStatement.executeQuery(sql)
          //                    stmt.addBatch(line.value())
          while (rs.next()) {
            val link_key = rs.getString(1) + ":" + rs.getString(2)
            val media_id = rs.getString(3)
            if (link_key != null && media_id != null) {
              if (!dictionaryMap.containsKey(link_key)) {
                dictionaryMap.put(link_key, media_id)
              }
            }
          }
        }
      }
    } catch {
      case t: Throwable => {
        logger.error(
          "save to db error!!!!!! stmt sql.............." + sql,
          t
        )
      }
    }

  }

  def readQRMapFromMysqlSplit(connMap: mutable.HashMap[String, Tuple6[String,
    String,
    String,
    String,
    String,
    String]],
                              sql: String): util.HashMap[String, String] = {
    val dictionaryMap = new util.HashMap[String, String]()
    //    val keys = connMap.keySet()
    //    var valueSet = Set[String]()
    //    keys.toArray.foreach(key => {
    //      val value = connMap.get(key.toString)
    //      valueSet += value
    //    })
    //读取wx默认库的字典
    readQRMapByStateMap(MysqlUtil.getConnection, sql, dictionaryMap)
    //读取qq默认库的字典
    readQRMapByStateMap(MysqlUtil.getQQConnection, sql, dictionaryMap)

    connMap.values.foreach(value => {
      try {
        //      println("collection value : "+ value)
        //      val connArray = value.split(":")
        val user = value._5
        val pwd = value._6
        val connName = value._1 + value._2
        val url = "jdbc:mysql://" + value._3 + ":" + value._4 + "/" + value._2 + "?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"

        readQRMapByStateMap(MysqlUtil.getOtherConnection(connName, url, user, pwd), sql, dictionaryMap)
      } catch {
        case t: Throwable => {
          logger.error(
            "save to db error!!!!!! stmt sql.............." + sql,
            t
          )
        }
      }
    })
    //    println("this collect is : "+valueSet)
    //    resultDF.select("*").show(10)
    dictionaryMap
  }

  //  def getConnection(): Connection ={
  //    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
  //    DriverManager getConnection("jdbc:phoenix:47.92.119.232:2181")
  //  }
  /**
   * 初始化Driver
   */
  try Class.forName(ConfigUtils.getProperty("database.driver"))
  catch {
    case e: ClassNotFoundException => e.printStackTrace()
  }

  def readFromMysql(sparkSession: SparkSession, table: String): DataFrame = {
    val url = ConfigUtils.getProperty("database.url")
    val user = ConfigUtils.getProperty("database.user")
    val password = ConfigUtils.getProperty("database.password")
    val driver = ConfigUtils.getProperty("database.driver")
    // 从 mysql 中读取数据
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
    jdbcDF
  }

  //  /**
  //    * 获取一个Hbase-Phoenix的连接
  //    *
  //    * @param host
  //    * zookeeper的master-host
  //    * @param port
  //    * zookeeper的master-port
  //    * @return
  //    */
  //  def getConnection() = {
  //    var cc: Connection = null
  //    val url = "jdbc:phoenix:47.92.119.232:2181"
  //    if (cc == null) try {
  //      // Phoenix DB不支持直接设置连接超时
  //      // 所以这里使用线程池的方式来控制数据库连接超时
  //      val exec = Executors.newFixedThreadPool(1)
  //      val call = new Callable[Connection] {
  //        override def call(): Connection = {
  //          DriverManager.getConnection(url)
  //        }
  //      }
  //      val future = exec.submit(call)
  //      // 如果在5s钟之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞，防止服务夯死
  //      cc = future.get(1000 * 5, TimeUnit.MILLISECONDS)
  //      exec.shutdownNow
  //    } catch {
  //      case e: InterruptedException => e.printStackTrace()
  //    }
  //    cc
  //  }

  /**
   * 根据host、port，以及sql查询hbase中的内容;根据phoenix支持的SQL格式，查询Hbase的数据，并返回json格式的数据
   *
   * @param host
   * zookeeper的master-host
   * @param port
   * zookeeper的master-port
   * @param phoenixSQL
   * sql语句
   * @return json-string
   * @return
   */
  def execSql(host: String, port: String, phoenixSQL: String): String = {
    if (host == null || port == null || (host.trim eq "") || (port.trim eq "")) {
      "必须指定hbase master的IP和端口"
    } else if (phoenixSQL == null || (phoenixSQL.trim eq "")) {
      "请指定合法的Phoenix SQL！"
    }
    var result = ""
    try { // 耗时监控：记录一个开始时间
      val startTime = System.currentTimeMillis
      // 获取一个Phoenix DB连接
      val conn = DBUtils.getConnection
      if (conn == null) return "Phoenix DB连接超时！"
      // 准备查询
      val stmt = conn.createStatement
      val set = stmt.executeQuery(phoenixSQL).asInstanceOf[PhoenixResultSet]
      // 查询出来的列是不固定的，所以这里通过遍历的方式获取列名
      val meta = set.getMetaData
      val cols = new util.ArrayList[String]
      while (set.next) {
        if (cols.size == 0) {
          var i = 1
          val count = meta.getColumnCount
          while (i <= count) {
            cols.add(meta.getColumnName(i))
            i += 1;
            i - 1
          }
        }
        result = cols.toString
      }

      // 耗时监控：记录一个结束时间
      val endTime = System.currentTimeMillis
    } catch {
      case e: SQLException => "SQL执行出错：" + e.getMessage
      case e: JSONException => "JSON转换出错：" + e.getMessage
    }
    result
  }

  //  def getDatasource(): DataSource = {
  //
  //    val ds = new DruidDataSource
  //    val prop = new Properties()
  //
  //    ds.setConnectProperties(prop)
  //    ds.setDriverClassName(ConfigUtils.getProperty("database.driver"))
  //       ds.setUsername("")
  //       ds.setPassword("")
  //    ds.setUrl(ConfigUtils.getProperty("database.url"))
  //    ds.setInitialSize(5) // 初始的连接数；
  //    ds.setMaxActive(100)
  //    //   ds.setMinIdle(minIdle)
  //    ds
  //  }
  def closeConn(connection: Connection) = {
    try {
      if (connection != null) {
        try {
          connection.close()
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }
      }
    }
  }

  def use[A <: {def close() : Unit}, B](resource: A)(code: A ⇒ B): B =
    try code(resource)
    finally resource.close()

}
