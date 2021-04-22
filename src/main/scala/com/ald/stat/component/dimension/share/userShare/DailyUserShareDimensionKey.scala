package com.ald.stat.component.dimension.share.userShare

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 用户分享    DimensionKey定义
  * src:分享来源，存储分享人的uu
  * @param logRecord 日志对象
  */
class DailyUserShareDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)
  override val key: String = logRecord.ak + ":" + logRecord.te + ":" + dayAndHour._1 + ":" + logRecord.src + ":" + logRecord.layer
}

object DailyUserShareDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyUserShareDimensionKey(c)
}





