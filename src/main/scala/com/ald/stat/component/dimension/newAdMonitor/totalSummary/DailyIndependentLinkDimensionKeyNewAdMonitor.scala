package com.ald.stat.component.dimension.newAdMonitor.totalSummary

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
 * 新版广告检测 - baseKey
 * @param logRecord
 * @author guo
 */
class DailyIndependentLinkDimensionKeyNewAdMonitor(logRecord: LogRecord) extends DimensionKey(logRecord: LogRecord) {

  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":"+ dayAndHour._1

  override lazy val hashKey: String = HashUtils.getHash(this.key).toString

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      false
    }
    else
      obj.asInstanceOf[DimensionKey].key.equalsIgnoreCase(key)
  }

  override def hashCode() = key.hashCode()

  override def toString: String = key
}

object DailyIndependentLinkDimensionKeyNewAdMonitor extends KeyTrait[LogRecord, DimensionKey] {
  implicit val dimensionKeySorting = new Ordering[DimensionKey] {
    override def compare(x: DimensionKey, y: DimensionKey): Int = x.key.compare(y.key)
  }

  def getKey(logRecord: LogRecord): DimensionKey = new DailyIndependentLinkDimensionKeyNewAdMonitor(logRecord)
}