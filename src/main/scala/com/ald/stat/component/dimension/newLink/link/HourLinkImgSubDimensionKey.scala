package com.ald.stat.component.dimension.newLink.link

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
  * 新增授权用户
  * @param logRecord
  */
class HourLinkImgSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2+ ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.uu + ":" + logRecord.ifo + ":" + logRecord.img
}

object HourLinkImgSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{
  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourLinkDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new HourLinkImgSubDimensionKey(c)
}









