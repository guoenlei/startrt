
  package com.ald.stat.component.dimension.qrCode.qrGroup

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

  /**
    * 二维码组统计 DimensionKey定义
    * Created by admin on 2018/6/6.
    */
  class HourQrGroupDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
    var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
    override val key: String = logRecord.ak + ":" + logRecord.te + ":" + dayAndHour._1 + ":" + dayAndHour._2 + ":" + logRecord.qr_group
  }

  object HourQrGroupDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

    override def getKey(c: LogRecord): DimensionKey = new HourQrGroupDimensionKey(c)
  }






