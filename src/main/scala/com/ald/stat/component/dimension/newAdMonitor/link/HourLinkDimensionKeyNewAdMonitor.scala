package com.ald.stat.component.dimension.newAdMonitor.link

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
 * 新版广告检测 - dimensionKey
 * Daily Dimension key  小时
 *
 * @author gel
 * @date 2019年9月16日
 */


class HourLinkDimensionKeyNewAdMonitor(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2 + ":" + logRecord.wsr_query_ald_link_key
}

object HourLinkDimensionKeyNewAdMonitor extends KeyTrait[LogRecord, DimensionKey] {

  override def getKey(c: LogRecord): DimensionKey = new HourLinkDimensionKeyNewAdMonitor(c)
}