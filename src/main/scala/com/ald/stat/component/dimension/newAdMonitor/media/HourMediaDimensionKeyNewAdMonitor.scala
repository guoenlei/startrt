package com.ald.stat.component.dimension.newAdMonitor.media

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
 * 新版广告检测 - baseKey
 * 媒体分析
 * Daily Dimension key  小时
 *
 * @author gel
 * @date 2019年9月16日
 */
class HourMediaDimensionKeyNewAdMonitor(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2+ ":" + logRecord.wsr_query_ald_media_id
}

object HourMediaDimensionKeyNewAdMonitor extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new HourMediaDimensionKeyNewAdMonitor(c)
}
