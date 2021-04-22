package com.ald.stat.component.dimension.newAdMonitor.amount

import com.ald.stat.component.dimension.newAdMonitor.totalSummary.DailyIndependentLinkDimensionKeyNewAdMonitor
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
 * UV
 * 新版广告检测 - dimensionKey
 * @param logRecord
 * @author guo
 */
class FullAmountDailyOpenidSubDimensionKeyNewAdMonitor(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord){
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.op
}
object FullAmountDailyOpenidSubDimensionKeyNewAdMonitor extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): FullAmountDailyOpenidSubDimensionKeyNewAdMonitor = new FullAmountDailyOpenidSubDimensionKeyNewAdMonitor(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyIndependentLinkDimensionKeyNewAdMonitor(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}