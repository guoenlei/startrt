package com.ald.stat.component.dimension.newAdMonitor.link

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
 * 新版广告检测 - dimensionKey
 * Daily Dimension key  天
 *
 * @author gel
 * @date 2019年9月16日
 */
class DailyLinkDimensionKeyNewAdMonitor(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.wsr_query_ald_link_key
}

object DailyLinkDimensionKeyNewAdMonitor extends KeyTrait[LogRecord, DimensionKey] {

  override def getKey(c: LogRecord): DimensionKey = new DailyLinkDimensionKeyNewAdMonitor(c)
}