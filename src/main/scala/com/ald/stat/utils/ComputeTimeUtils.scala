package com.ald.stat.utils

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date, TimeZone}

import com.ald.stat.DayUtils
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

object ComputeTimeUtils {

  //时间校正,差3秒钟将不校正
  def timesDiffence(serverTime: Long, clientTime: Long): Long = {
    val diff = toMillsecond(serverTime) - toMillsecond(clientTime)
    if (Math.abs(diff) < 3000) 0
    else
      diff
  }

  def toMillsecond(timestamp: Long): Long = {
    var timeLong = timestamp
    if (timestamp.toString.length == 10) timeLong = timestamp * 1000
    timeLong
  }


  def getDateStr(timestamp: Long): String = {
    val d = new Date(toMillsecond(timestamp))
    formatDate(d, null)
  }

  def getDateStrAndHour(timestamp: Long): (String, String) = {
    val d = new Date(toMillsecond(timestamp))
    (formatDate(d, null), formatDate(d, "HH"))
  }

  def getDateStrAndHm(timestamp: Long): (String, String) = {
    val d = new Date(toMillsecond(timestamp))
    (formatDate(d, null), formatDate(d, "HH:mm"))
  }

  def getDateStrByInt(date: String, days: Int): String = {
    val currentDay = DateUtils.parseDate(date, "yyyyMMdd")
    val d = DateUtils.addDays(currentDay, days)
    formatDate(d, null)
  }

  def formatDate(date: Date, pattern: String): String = {
    var formatDate = ""
    if (pattern != null && pattern.length > 0) formatDate = DateFormatUtils.format(date, pattern)
    else formatDate = DateFormatUtils.format(date, "yyyyMMdd")
    formatDate
  }

  def getYesterDayStr(dayStr: String): String = {
    DayUtils.getCalDays(dayStr, -1)
    //    LocalDate.parse(dayStr,DateTimeFormatter.BASIC_ISO_DATE).plusDays(-1).format(DateTimeFormatter.BASIC_ISO_DATE)
  }

  /**
    * 获取指定日期的前一天的字符串值
    * @param date
    * @param num 想得到前n天用负数，想得到后n天用正数
    * @param pattern
    * @return
    */
  def getDayStr(date: Date, num:Int, pattern:String): String = {
    val dateStr = getDateStr(date)
    DayUtils.getCalDays(dateStr,num,pattern)
  }

  /**
    * 获取指定日期的前一天的字符串值
    * @param date
    * @param pattern 指定格式
    * @return
    */
  def getYesterDayStr(date: Date,pattern:String): String = {
    val dateStr = getDateStr(date)
    DayUtils.getCalDays(dateStr,-1,pattern)
  }

  def getDateStr(date: Date): String = {
    DayUtils.formatDate(date, null)
    //    date.format(DateTimeFormatter.BASIC_ISO_DATE)
  }

  def getDayAndHourStr(date: Date): String = {
    DayUtils.formatDate(date, "yyyyMMddHH")
  }

  def getYesterDayTimeStamp(date: Date): Long = {
    DateUtils.truncate(DateUtils.addDays(date, -1), Calendar.DAY_OF_MONTH).getTime
    //    date.plusDays(-1).atTime(0,0,0).toEpochSecond(ZoneOffset.ofHours(8))*1000
  }

  def inDay(dayStr: String, eventTime: Long): Boolean = {
    val currentDay = DateUtils.parseDate(dayStr, "yyyyMMdd")
    //    val currentDay = LocalDate.parse(dayStr,DateTimeFormatter.BASIC_ISO_DATE)
    val currentDayStart = getTodayDayTimeStamp(currentDay)
    val tomorrowDatStart = getTomorowDayTimeStamp(currentDay)
    if (eventTime >= currentDayStart && eventTime < tomorrowDatStart) true
    else false
  }

  def getTomorowDayTimeStamp(date: Date): Long = {
    DateUtils.truncate(DateUtils.addDays(date, 1), Calendar.DAY_OF_MONTH).getTime
    //    date.plusDays(1).atTime(0,0,0).toEpochSecond(ZoneOffset.ofHours(8))*1000
  }

  def getTodayDayTimeStamp(date: Date): Long = {
    DateUtils.truncate(date, Calendar.DAY_OF_MONTH).getTime
    //    date.atTime(0,0,0).toEpochSecond(ZoneOffset.ofHours(8))*1000
  }

  def betweenStrDays(dateStartStr: String, dateEndStr: String) = {
    val dateStart = DateUtils.parseDate(dateStartStr, "yyyyMMdd")
    val dateEnd = DateUtils.parseDate(dateEndStr, "yyyyMMdd")
    betweenDays(dateStart, dateEnd)
  }

  def betweenDays(dateStart: Date, dateEnd: Date): Long = {
    try {
      val diff = dateStart.getTime - dateEnd.getTime
      (TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        0l
    }
  }

  /**
    * yyyyMMdd转时间戳
    * @param tm
    * @return
    */
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime()/1000
    tim
  }

  /**
    * 获取redis key的失效时间
    * 设置为次日凌晨2时
    * @return
    */
  def getRepireKeyTime():Long = LocalDate.now.atTime(2, 0).plusDays(1).atZone(TimeZone.getTimeZone("GMT+0800").toZoneId).toEpochSecond


  /**
    * 校正时间
    * 如果et的日期与系统日期不匹配，则将系统日期赋给et
    * @param et
    * @param dateStr
    */
  def checkTime(et:Long,dateStr:Long):String ={
    if(getDateStr(et) != getDateStr(dateStr))
      dateStr.toString
    else et.toString
  }

  /**
    * 获取相对于今天某天(相聚多少天)日期的指定格式
    *
    * @param day     0 今天，1 明天，-1 昨天
    * @param pattern 日期格式 正则
    * @param year    几年后
    * @param month   几月后
    * @param week    1 表示7天后的日期
    * @return 例如yyyy-MM-dd(指定规则pattern)
    */
  def getUdfDate(day: Int, pattern: String, year: Int = 0, month: Int = 0, week: Int = 0): String = {
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
    LocalDate.now().plusYears(year).plusMonths(month).plusWeeks(week).plusDays(day).format(dtf)
  }

  /**
    * 获取指定日期day，往前numDays天的hdfs目录path所组成的数组
    *
    * @param day     指定日期
    * @param numDays 要获取的天数
    * @return 包含hdfs多个路径的数组
    */
  def getUdfDaysDirs(path: String, day: String, numDays: Int): Array[String] = {
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    //要读取的日志目录数组
    var dirs = Array[String]()
    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt)

    for (i <- -numDays + 1 to 0) {
      val logDir = path + "/" + date.plusDays(i).format(dtf)
      println(s"读取$logDir/*/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*/*")
    }
    dirs
  }

  /**
    * 弹性mr日志目录，获取指定日期往前7天或30天的数据路径的数组
    * cosn://aldwxlogbackup/log_parquet/zzcg-etl-VM-0-224-ubuntu2017120313/
    * cosn://aldwxlogbackup/log_parquet/星2017120313/
    */
  def getTencentDirs(day:String,numDays:Int): Array[String] ={
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt)
    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"
    var dirs = Array[String]()
    for (i <- -numDays + 1 to 0) {
      val logDir =s"$hdfsUrl${date.plusDays(i).format(dtf)}*"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*")
    }
    dirs
  }

  /**
    * 获取erm指定天数的数据集合，可以和上面获取数据的方法整合成一个，后续再改吧
    * @param day 从指定的日期开始算，默认是昨天
    * @param daysArr 数组里面存要获取的天数，-1表示1天前
    * @return 返回要获取的所有天数的emr数据路径的数组
    */
  def getSpecifyTencentDirs(day:String,daysArr:Array[Int]): Array[String] ={
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt)
    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"
    var dirs = Array[String]()
    for (i <- daysArr) {
      val logDir =s"$hdfsUrl${date.plusDays(-i).format(dtf)}*"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*")
    }
    dirs
  }

  /**
    * 获取次日凌晨的时间戳
    * @return
    */
  def getMoningTimeStamp(day_wz: String) : Long = {
    val dayTime = day_wz
//    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val a = dateFormat.parse(dateFormat.format(new Date())).getTime
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val dt = fm.parse(dayTime)
   getTomorowDayTimeStamp(dt)
  }

  /**
    * 获取今日凌晨的时间戳
    * @return
    */
  def getTodayMoningTimeStamp(day_wz: String) : Long = {
    val dayTime = day_wz
    //    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //    val a = dateFormat.parse(dateFormat.format(new Date())).getTime
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val dt = fm.parse(dayTime)
    getTodayDayTimeStamp(dt)
  }

  /**
    * 时间戳转换成日期
    * @param tm
    * @return
    */
  def tranTimeToString(tm:Long) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }

  /**
    * yyyy-MM-dd转时间戳
    * @param tm
    * @return
    */
  def tranTimeToLongNext(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime()
    tim
  }
  def main(args: Array[String]): Unit = {

//    println(getTodayDayTimeStamp(new Date()))
//
//    println(checkTime(1533362082,getTodayDayTimeStamp(new Date())))

    println(getTodayMoningTimeStamp("2019-06-15"))
  }
}
