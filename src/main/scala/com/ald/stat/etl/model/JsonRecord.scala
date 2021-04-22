package com.ald.stat.etl.model

import com.alibaba.fastjson.JSON
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty

/**
  * Json日志映射
  * Created by zhaofw on 2018/8/25.
  */
class JsonRecord extends Serializable{
  @BeanProperty
  var ak: String = _
  @BeanProperty
  var waid: String = _
  @BeanProperty
  var wst: String = _
  @BeanProperty
  var uu: String = _ //用户ID
  @BeanProperty
  var st: String = _ //app启动的时间 end time  13位时间戳
  @BeanProperty
  var ag: String = _ //上次访问的访问时间
  @BeanProperty
  var ag_ald_link_key: String = _
  @BeanProperty
  var ag_ald_media_id: String = _
  @BeanProperty
  var ag_ald_position_id: String = _
  @BeanProperty
  var wsr_query_ald_link_key: String = _
  @BeanProperty
  var wsr_query_ald_media_id: String = _
  @BeanProperty
  var wsr_query_ald_position_id: String = _
  @BeanProperty
  var ag_aldsrc: String = _
  @BeanProperty
  var at: String = _
  @BeanProperty
  var wsr: String = _
  @BeanProperty
  var ev: String = _ // 事件类型0:_trackPageView的事件类型；
  @BeanProperty
  var life: String = _
  //小程序错误次数
  @BeanProperty
  var ec: String = _
  //网络类型
  @BeanProperty
  var nt: String = _
  //手机类型
  @BeanProperty
  var pm: String = _
  //微信基础框架版本
  @BeanProperty
  var wsdk: String = _
  // 手机屏幕像素点
  @BeanProperty
  var pr: String = _
  //手机屏幕款度
  @BeanProperty
  var ww: String = _
  //手机屏幕高度
  @BeanProperty
  var wh: String = _
  @BeanProperty
  var lang: String = _
  @BeanProperty
  var wv: String = _
  //地理位置-经度
  @BeanProperty
  var lat: String = _
  //地理位置-纬度
  @BeanProperty
  var lng: String = _
  //速度
  @BeanProperty
  var spd: String = _
  //SDK版本
  @BeanProperty
  var v: String = _
  //客户端操作系统版本
  @BeanProperty
  var sv: String = _
  //客户端平台
  @BeanProperty
  var wvv: String = _
  //是否首次打开小程序
  @BeanProperty
  var ifo: String = _
  @BeanProperty
  var city: String = _
  @BeanProperty
  var client_ip: String = _
  @BeanProperty
  var ct: String = _
  @BeanProperty
  var day: String = _
  @BeanProperty
  var dr: String = _ //session 长度 毫秒
  @BeanProperty
  var error_messsage: String = _ //
  @BeanProperty
  var hour: String = _ ////
  @BeanProperty
  var ifp: String = _
  @BeanProperty
  var lp: String = _
  @BeanProperty
  var path: String = _
  @BeanProperty
  var pp: String = _
  @BeanProperty
  var province: String = _ //10位数以内的随机整数
  @BeanProperty
  var qr: String = _ // url
  @BeanProperty
  var scene: String = _ //refer
  @BeanProperty
  var scene_group_id: String = _
  @BeanProperty
  var et: String = _ //记录上报时间 13位时间戳
  @BeanProperty
  var tp: String = _
  @BeanProperty
  var wsr_query_ald_share_src: String = _
  @BeanProperty
  var wsr_query_aldsrc: String = _
  @BeanProperty
  var ct_chain: String = _
  @BeanProperty
  var ct_path: String = _

  /**
    * 品牌
    */
  @BeanProperty
  var brand: String = _

  /**
    * 型号
    */
  @BeanProperty
  var model: String = _

  /**
    * 分享源
    */
  @BeanProperty
  var src: String = _

  /**
    * 二维码组
    */
  @BeanProperty
  var qr_group: String = _

  @BeanProperty
  var ct_key: String = _

  @BeanProperty
  var ct_value: String = _

  override def clone(): JsonRecord = {
    JSON.parseObject(JSON.toJSONString(this, true), classOf[JsonRecord])
  }


  override def toString = s"JsonRecord($ak, $waid, $wst, $uu, $st, $ag, $ag_ald_link_key, $ag_ald_media_id, $ag_ald_position_id, $wsr_query_ald_link_key, $wsr_query_ald_media_id, $wsr_query_ald_position_id, $ag_aldsrc, $at, $wsr, $ev, $life, $ec, $nt, $pm, $wsdk, $pr, $ww, $wh, $lang, $wv, $lat, $lng, $spd, $v, $sv, $wvv, $ifo, $city, $client_ip, $ct, $day, $dr, $error_messsage, $hour, $ifp, $lp, $path, $pp, $province, $qr, $scene, $scene_group_id, $et, $tp, $wsr_query_ald_share_src, $wsr_query_aldsrc, $ct_chain, $ct_path, $brand, $model, $src, $qr_group,$ct_key,$ct_value,$clone)"
}

object JsonRecord extends Serializable {
  val logger = LoggerFactory.getLogger("JsonRecord")

  def logBean(line: String): JsonRecord = {
    try {
      val LogRecord = JSON.parseObject(line, classOf[JsonRecord])
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
