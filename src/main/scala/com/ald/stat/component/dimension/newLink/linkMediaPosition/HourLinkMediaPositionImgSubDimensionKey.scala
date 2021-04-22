package com.ald.stat.component.dimension.newLink.linkMediaPosition

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}
import org.apache.commons.lang3.StringUtils

/**
  * 新增授权用户
  * @param logRecord
  */
class HourLinkMediaPositionImgSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  var uuOrOpenId = logRecord.uu
  if(StringUtils.isNotBlank(logRecord.op) && logRecord.op != "useless_openid"){
    uuOrOpenId = logRecord.op
  }
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.wsr_query_ald_media_id + ":" + logRecord.wsr_query_ald_position_id + ":" + uuOrOpenId + ":" + logRecord.ifo + ":" + logRecord.img
}

object HourLinkMediaPositionImgSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{
  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourLinkMediaPositionDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new HourLinkMediaPositionImgSubDimensionKey(c)
}
