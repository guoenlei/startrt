package com.ald.stat.component.dimension.newLink.amount

import com.ald.stat.component.dimension.IndependentLink.HourIndependentLinkDimensionKey
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
  * Created by root on 2018/5/23.
  */
class FullAmountHourOpenidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord){
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.op
}
object FullAmountHourOpenidSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): FullAmountHourOpenidSubDimensionKey = new FullAmountHourOpenidSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourIndependentLinkDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}
