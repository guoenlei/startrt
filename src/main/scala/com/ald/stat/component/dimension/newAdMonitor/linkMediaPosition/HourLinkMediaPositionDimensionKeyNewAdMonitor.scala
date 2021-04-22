
package com.ald.stat.component.dimension.newAdMonitor.linkMediaPosition

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
 * 外链分析
 * 活动+场景值+渠道+小时
 * 新版广告检测 - baseKey
 *
 * @author gel
 * @date 2019年9月16日
 */
class HourLinkMediaPositionDimensionKeyNewAdMonitor(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.hour + ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.wsr_query_ald_media_id + ":" + logRecord.wsr_query_ald_position_id
}

object HourLinkMediaPositionDimensionKeyNewAdMonitor extends KeyTrait[LogRecord, DimensionKey] {

  override def getKey(c: LogRecord): DimensionKey = new HourLinkMediaPositionDimensionKeyNewAdMonitor(c)
}
