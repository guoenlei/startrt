package com.ald.stat.component.dimension.IndependentLink

/**
  * Created with IntelliJ IDEA.
  * User: @ziyu  freedomziyua@gmail.com
  * Date: 2019-08-30
  * Time: 15:25
  * Description: 
  */
import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

class DailyIndependentLinkDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord: LogRecord) {

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

object DailyIndependentLinkDimensionKey extends KeyTrait[LogRecord, DimensionKey] {
  implicit val dimensionKeySorting = new Ordering[DimensionKey] {
    override def compare(x: DimensionKey, y: DimensionKey): Int = x.key.compare(y.key)
  }

  def getKey(logRecord: LogRecord): DimensionKey = new DailyIndependentLinkDimensionKey(logRecord)
}

