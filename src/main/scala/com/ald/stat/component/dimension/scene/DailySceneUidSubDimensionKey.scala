package com.ald.stat.component.dimension.scene

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * subKey  ak:yyyymmdd:hh:uu:at
  */
class DailySceneUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + logRecord.te + ":" + dayHour._1 + ":" + logRecord.scene_group_id + ":" + logRecord.scene + ":" + logRecord.uu
}

object DailySceneUidSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailySceneDimensionKey(k.lr)

  /**
    * 获得纬度key中hh:uu
    *
    * @param k
    * @return
    */
  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new DailySceneUidSubDimensionKey(c)

}

