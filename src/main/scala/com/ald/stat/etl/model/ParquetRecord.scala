package com.ald.stat.etl.model

import com.alibaba.fastjson.JSON
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty

/**
  * 用于写入parquet文件
  * Created by zhaofw on 2018/8/27.
  */
class ParquetRecord extends Serializable{
  @BeanProperty
  var ak: String = _  //阿拉丁app_key

  @BeanProperty
  var uu: String = _  //用户id

  @BeanProperty
  var att: String = _ //sessionId 由于kylin对"at"字段敏感，所以把原来的at需改为att

  @BeanProperty
  var pp: String = _  //页面路径

  @BeanProperty
  var ifo: String = _ //新用户标识 true||false

  @BeanProperty
  var tp: String = _  //事件名称  当ev=event时，tp字段存储该事件的具体名称(ald_share_statues||ald_share_click||ald_reachbottom......)

  @BeanProperty
  var ald_day: String = _ //日期，创建hive表的分区字段

  @BeanProperty
  var ev: String = _  //日志级别  app||page||event

  @BeanProperty
  var scene: String = _ //场景值id

  @BeanProperty
  var scene_group: String = _ //场景值组id

  @BeanProperty
  var qr: String = _  //二维码id

  @BeanProperty
  var qr_group: String = _  //二维码组id

  @BeanProperty
  var link_key: String = _  //外链id

  @BeanProperty
  var media_id: String = _  //媒体id

  @BeanProperty
  var link_scene: String = _  //外链的场景值id

  @BeanProperty
  var province: String = _  //省份

  @BeanProperty
  var city: String = _  //城市

  @BeanProperty
  var phone_model: String = _ //手机型号  对应于日志中的pm字段

  @BeanProperty
  var phone_brand: String = _ //手机品牌

  @BeanProperty
  var wvv: String = _ //操作系统 Android||IOS||Devtools

  @BeanProperty
  var wv: String = _  //微信版本

  @BeanProperty
  var nt: String = _  //网络类型

  @BeanProperty
  var path: String = _ //页面分享path

  @BeanProperty
  var ct: String = _ //事件参数

  @BeanProperty
  var ct_key: String = _ //事件参数名称

  @BeanProperty
  var ct_value: String = _ //事件参数的值

  override def clone(): ParquetRecord = {
    JSON.parseObject(JSON.toJSONString(this, true), classOf[ParquetRecord])
  }

  override def toString = s"ParquetRecord($ak,$uu,$att,$pp,$ifo,$tp,$ald_day,$ev,$scene,$scene_group,$qr,$qr_group," +
    s"$link_key,$media_id,$link_scene,$province,$city,$phone_model,$phone_brand,$wvv,$wv,$nt,$path,$ct,$ct_key,$ct_value,$clone)"

  object ParquetRecord extends Serializable {
    val logger = LoggerFactory.getLogger("ParquetRecord")
    def logBean(line: String): ParquetRecord = {
      try {
        val LogRecord = JSON.parseObject(line, classOf[ParquetRecord])
        if (LogRecord != null) {
          LogRecord
        } else {
          null
        }
      }
      catch {
        case ex: Throwable => {
          logger.error(s"convert exeception $line", ex)
          null
        }
      }
    }
  }

}
