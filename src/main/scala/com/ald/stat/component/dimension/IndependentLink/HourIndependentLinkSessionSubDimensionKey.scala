package com.ald.stat.component.dimension.IndependentLink

/**
  * Created with IntelliJ IDEA.
  * User: @ziyu  freedomziyua@gmail.com
  * Date: 2019-08-30
  * Time: 14:18
  * Description: 
  */


import com.ald.stat.component.dimension._
import com.ald.stat.component.dimension.trend.HourDimensionKey
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * Key  ak:yyyymmdd:hh:at
  */
class HourIndependentLinkSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.at
}

object HourIndependentLinkSessionSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): HourIndependentLinkSessionSubDimensionKey = new HourIndependentLinkSessionSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourIndependentLinkDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}



