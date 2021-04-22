package com.ald.stat.component.dimension.newAdMonitor.link;

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
 * UV
 * 新版广告检测 - dimensionKey
 * @param logRecord
 * @author guo
 */
class HourLinkUidSubDimensionKeyNewAdMonitor(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1+ ":" + dayHour._2 + ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.uu
}

object HourLinkUidSubDimensionKeyNewAdMonitor extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourLinkDimensionKeyNewAdMonitor(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new HourLinkUidSubDimensionKeyNewAdMonitor(c)
}