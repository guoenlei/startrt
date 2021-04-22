package com.ald.stat.component.dimension.newAdMonitor.amount;

import com.ald.stat.component.dimension.newAdMonitor.totalSummary.HourIndependentLinkDimensionKeyNewAdMonitor
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
 * UV
 * 新版广告检测 - dimensionKey
 * @param logRecord
 * @author guo
 */
class FullAmountHourOpenidSubDimensionKeyNewAdMonitor(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord){
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.op
}
object FullAmountHourOpenidSubDimensionKeyNewAdMonitor extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): FullAmountHourOpenidSubDimensionKeyNewAdMonitor = new FullAmountHourOpenidSubDimensionKeyNewAdMonitor(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourIndependentLinkDimensionKeyNewAdMonitor(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}
