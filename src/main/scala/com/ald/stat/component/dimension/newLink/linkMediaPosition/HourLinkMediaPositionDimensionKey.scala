package com.ald.stat.component.dimension.newLink.linkMediaPosition

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 外链分析
  * 活动+场景值+渠道+小时
  * Created by lmw on 2018/12/12.
  */
class HourLinkMediaPositionDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.hour + ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.wsr_query_ald_media_id + ":" + logRecord.wsr_query_ald_position_id
}

object HourLinkMediaPositionDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new HourLinkMediaPositionDimensionKey(c)
}
