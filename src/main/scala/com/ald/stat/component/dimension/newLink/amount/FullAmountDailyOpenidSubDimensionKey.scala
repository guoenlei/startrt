package com.ald.stat.component.dimension.newLink.amount

import com.ald.stat.component.dimension.IndependentLink.DailyIndependentLinkDimensionKey
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
  * Created by root on 2018/5/23.
  */
class FullAmountDailyOpenidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord){
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.op
}
object FullAmountDailyOpenidSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): FullAmountDailyOpenidSubDimensionKey = new FullAmountDailyOpenidSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyIndependentLinkDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}
