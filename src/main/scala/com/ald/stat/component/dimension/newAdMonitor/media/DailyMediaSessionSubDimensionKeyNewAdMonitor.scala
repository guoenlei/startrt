package com.ald.stat.component.dimension.newAdMonitor.media;

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
 * Session
 * 新版广告检测 - dimensionKey
 * @param logRecord
 * @author guo
 */
class DailyMediaSessionSubDimensionKeyNewAdMonitor(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.wsr_query_ald_media_id + ":" + logRecord.at
}

object DailyMediaSessionSubDimensionKeyNewAdMonitor extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyMediaDimensionKeyNewAdMonitor(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new DailyMediaSessionSubDimensionKeyNewAdMonitor(c)

}
