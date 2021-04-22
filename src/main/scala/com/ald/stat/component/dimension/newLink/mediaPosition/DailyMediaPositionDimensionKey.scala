package com.ald.stat.component.dimension.newLink.mediaPosition

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 渠道+场景值+天
  * Created by lmw on 2018/12/12.
  */
class DailyMediaPositionDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1+ ":" + logRecord.wsr_query_ald_media_id + ":" + logRecord.wsr_query_ald_position_id
}

object DailyMediaPositionDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyMediaPositionDimensionKey(c)
}
