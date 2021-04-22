package com.ald.stat.component.dimension.newAdMonitor.summary

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
 * 新版广告检测 - baseKey - 头部汇总
 * @author gel
 * @date 2019年9月16日
 */
class HourDimensionKeyNewAdMonitor(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2
}

object HourDimensionKeyNewAdMonitor extends KeyTrait[LogRecord, DimensionKey] {

  override def getKey(c: LogRecord): DimensionKey = new HourDimensionKeyNewAdMonitor(c)
}
