package com.ald.stat.component.dimension.newLink.linkPosition

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


class HourLinkPositionSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.wsr_query_ald_position_id + ":" + logRecord.at
}

object HourLinkPositionSessionSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourLinkPositionDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new HourLinkPositionSessionSubDimensionKey(c)
}
