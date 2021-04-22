package com.ald.stat.component.dimension.newLink.linkMedia

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 外链分析
  *  活动+渠道+天
  * Created by lmw on 2018/12/12.
  */
class DailyLinkMediaDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1+ ":" + logRecord.wsr_query_ald_link_key + ":" + logRecord.wsr_query_ald_media_id
}

object DailyLinkMediaDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyLinkMediaDimensionKey(c)
}