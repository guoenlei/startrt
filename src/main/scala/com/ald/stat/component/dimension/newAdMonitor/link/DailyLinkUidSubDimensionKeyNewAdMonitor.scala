package com.ald.stat.component.dimension.newAdMonitor.link

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.component.dimension.newLink.link.{DailyLinkDimensionKey, DailyLinkUidSubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
 * UV
 * 新版广告检测 - dimensionKey
 * @param logRecord
 * @author guo
 */
class DailyLinkUidSubDimensionKeyNewAdMonitor(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.op
}

object DailyLinkUidSubDimensionKeyNewAdMonitor extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyLinkDimensionKeyNewAdMonitor(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new DailyLinkUidSubDimensionKeyNewAdMonitor(c)
}