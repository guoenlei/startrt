package com.ald.stat.component.dimension.newLink.linkMediaPosition

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}
import org.apache.commons.lang3.StringUtils

/**
  * 新增授权用户
  * @param logRecord
  */
class HourLinkMediaPositionImgOpenidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);

  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.wsr_query_ald_media_id + ":" + logRecord.wsr_query_ald_position_id + ":" + logRecord.op  + ":" + logRecord.img
}

object HourLinkMediaPositionImgOpenidSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{
  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourLinkMediaPositionDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new HourLinkMediaPositionImgOpenidSubDimensionKey(c)
}
