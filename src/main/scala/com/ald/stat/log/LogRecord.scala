package com.ald.stat.log

import com.ald.stat.utils.{ComputeTimeUtils, SDKVersionUtils, StrUtils}
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty
import scala.util.Random

/**
  * ev="app" 、life:[launch|show|hide]
  *
  * 将日志的每一行转换成标准的对象，由于考虑到传递，所以需要序列化
  *
  */
class LogRecord extends Serializable {

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
  var wsr_query_ald_link_name: String = _
  @BeanProperty
  var ag_aldsrc: String = _
  @BeanProperty
  var at: String = _ // 小程序这个生命周期的Session
  @BeanProperty
  var wsr: String = _
  @BeanProperty
  var ev: String = _ // 事件类型0:_trackPageView的事件类型；

  // 生命周期 ev="app" 、life:[launch|show|hide]
  //ev="page",life:[head|unload|]
  //ev ="" 其他
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
  var pp: String = _ // 当前页面
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

  /**
    * 授权标识
    */
  @BeanProperty
  var img: String = _

  /**
    * 分享层级数
    */
  @BeanProperty
  var layer: String = _

  /**
    * 性别
    */
  @BeanProperty
  var gender: String = _


  /**
    * 性别嵌套在ufo中
    */
  @BeanProperty
  var ufo: String = _

  /**
    * openId
    */
  @BeanProperty
  var op: String = _
  /**
    * 二维码、外链、页面分享的utm参数
    */
  @BeanProperty
  var ct_utm_s: String = _
  @BeanProperty
  var ct_utm_m: String = _
  @BeanProperty
  var ct_utm_p: String = _
  @BeanProperty
  var wsr_query_utm_s: String = _
  @BeanProperty
  var wsr_query_utm_p: String = _
  @BeanProperty
  var wsr_query_utm_m: String = _
  /**
    * 微信和qq的区别标识字段
    */
  @BeanProperty
  var te: String = _


  override def clone(): LogRecord = {
    JSON.parseObject(JSON.toJSONString(this, true), classOf[LogRecord])
  }


  override def toString = s"LogRecord($ak, $waid, $wst, $uu, $st, $ag, $ag_ald_link_key, $ag_ald_media_id, $ag_ald_position_id, $wsr_query_ald_link_key, $wsr_query_ald_media_id, $wsr_query_ald_position_id, $wsr_query_ald_link_name, $ag_aldsrc, $at, $wsr, $ev, $life, $ec, $nt, $pm, $wsdk, $pr, $ww, $wh, $lang, $wv, $lat, $lng, $spd, $v, $sv, $wvv, $ifo, $city, $client_ip, $ct, $day, $dr, $error_messsage, $hour, $ifp, $lp, $path, $pp, $province, $qr, $scene, $scene_group_id, $et, $tp, $wsr_query_ald_share_src, $wsr_query_aldsrc, $ct_chain, $ct_path, $brand, $model, $src, $qr_group,$img,  $clone,$layer，$gender, ${ufo}, ${op}, $ct_utm_s, $ct_utm_m, $ct_utm_p, $wsr_query_utm_s, $wsr_query_utm_p, $wsr_query_utm_m, $te)"
}

object LogRecord extends Serializable {
  val logger = LoggerFactory.getLogger("LogRecord")

  def line2Bean(line: String): LogRecord = {
    try {
      val logRecord = JSON.parseObject(line, classOf[LogRecord])
      if (logRecord != null) {
        if (StringUtils.isNotBlank(logRecord.wsr) && logRecord.wsr != "null" && logRecord.wsr.isInstanceOf[String] && logRecord.wsr.startsWith("{") && logRecord.wsr.endsWith("}")) {
          val wsr = JSON.parseObject(logRecord.wsr, classOf[Wsr])
          if (StringUtils.isBlank(logRecord.scene) || logRecord.scene == "null") {
            logRecord.scene = wsr.scene //从wsr中获取scene字段
          }
          // TODO: 关于path的逻辑，需要放到ETL进行处理。
          if (StringUtils.isNotBlank(logRecord.ct_path) && logRecord.ct_path != "null") {
            //用于页面分享，如果ct_path不为null，则优先使用ct_path
            //ct_path 是新版(7.0)SDK的当前页面   path是进入小程序的起始页面   使用wsr.path计算页面分享是不正确的。
            //因为点击分享链接进来的，path中没有参数，所以这里只要路径，不要参数
            logRecord.path = logRecord.ct_path.split("\\?")(0)
            if(logRecord.ct_path.indexOf("utm_s") != -1){
              val splits = logRecord.ct_path.split("&")
              for(i <- splits){
                if(i.indexOf("utm_s") != -1){
                  if(i.split("=").length == 2){
                    logRecord.ct_utm_s = i.split("=")(1)
                  }
                }
              }
            }
          }
          //从wsr中获取path字段
          if (StringUtils.isBlank(logRecord.path) || logRecord.path == "null") {
            logRecord.path = wsr.path
          }
        }
        if(StringUtils.isNotBlank(logRecord.ufo) && logRecord.ufo != "null"){
          val user_info = JSON.parseObject(logRecord.ufo, classOf[UserInfo])
          if(StringUtils.isNotBlank(user_info.userInfo)){
            val ufo = JSON.parseObject(user_info.userInfo, classOf[Ufo])
            if(StringUtils.isNotBlank(ufo.gender)){
              logRecord.gender = ufo.gender
            }
          }
        }

        if(StringUtils.isBlank(logRecord.ct_utm_s)){
          logRecord.ct_utm_s = "null"
        }
        if(StringUtils.isBlank(logRecord.ct_utm_p)){
          logRecord.ct_utm_p = "null"
        }
        if(StringUtils.isBlank(logRecord.ct_utm_m)){
          logRecord.ct_utm_m = "null"
        }
        if(StringUtils.isBlank(logRecord.wsr_query_utm_p)){
          logRecord.wsr_query_utm_p = "null"
        }
        if(StringUtils.isBlank(logRecord.wsr_query_utm_s)){
          logRecord.wsr_query_utm_s = "null"
        }
        if(StringUtils.isBlank(logRecord.wsr_query_utm_m)){
          logRecord.wsr_query_utm_m = "null"
        }
        //utm参数的添加
        if(StringUtils.isBlank(logRecord.qr)){
          if(logRecord.scene == "1013" ||
            logRecord.scene == "1012" ||
            logRecord.scene == "1011" ||
            logRecord.scene == "1049" ||
            logRecord.scene == "1072" ||
            logRecord.scene == "1047" ||
            logRecord.scene == "1048"){
            if(StringUtils.isNotBlank(logRecord.wsr_query_utm_s) && logRecord.wsr_query_utm_s != "null"){
              logRecord.qr = logRecord.wsr_query_utm_s
              if(StringUtils.isNotBlank(logRecord.wsr_query_utm_m) && logRecord.wsr_query_utm_m != "null"){
                logRecord.qr_group = logRecord.wsr_query_utm_m
              }
            }
          }
        }
        if(StringUtils.isBlank(logRecord.wsr_query_ald_link_key)){
          if(logRecord.scene == "1058"||
            logRecord.scene == "1035"||
            logRecord.scene == "1014"||
            logRecord.scene == "1038"){
            if(logRecord.wsr_query_utm_s != "null" && logRecord.wsr_query_utm_m != "null"){
              logRecord.wsr_query_ald_link_key = logRecord.wsr_query_utm_s
              logRecord.wsr_query_ald_media_id = logRecord.wsr_query_utm_m
            }
          }
        }
        //如果img为null,则将其赋值为"useless_img"
        if(StringUtils.isBlank(logRecord.img) || logRecord.img == "null"){
          logRecord.img = "useless_img"
        }
        //如果ifo为null，则将其赋值为false
        if(StringUtils.isBlank(logRecord.ifo) || logRecord.ifo == "null"){
          logRecord.ifo = "false"
        }
        if(logRecord.img != "useless_img" && logRecord.ifo == "true"){//如果img != useless_img，并且ifo=true，则说明这条记录是一条新增授权的记录，这是一条我们需要的记录
          if(StringUtils.isBlank(logRecord.dr) || logRecord.dr == "null") logRecord.dr = "0" //此时将dr,ev，pp,赋值为无意义的值，目的是保证这条记录不会被过滤掉
          if(StringUtils.isBlank(logRecord.ev) || logRecord.ev == "null") logRecord.ev = "useless_level"
          if(StringUtils.isBlank(logRecord.pp) || logRecord.pp == "null") logRecord.pp = "useless_page"
        }
        if(StringUtils.isBlank(logRecord.te) || logRecord.te == "null"){
          logRecord.te = "wx"
        }

        //如果openId是null，则将其赋值为"useless_openid"
        if(StringUtils.isBlank(logRecord.op) || logRecord.op == "null"){
          logRecord.op = "useless_openid"
        }
        if(StringUtils.isBlank(logRecord.v) || logRecord.v == "null"){
          logRecord.v = "7.0.0"
        }

        //处理dr 毫秒=>秒
        if (StringUtils.isNotBlank(logRecord.dr) && logRecord.dr != "null") {
          try {
            //7.0.0 版本需要进行处理毫秒到秒 GlobalConstants.ALD_SDK_V_7 == logRecord.v
            if (SDKVersionUtils.isOldSDk(logRecord.v) == false) {
              logRecord.dr = (logRecord.dr.toLong / 1000).toString
            }
          } catch {
            case _: Throwable => {
              logRecord.dr = "0"
            }
          }
        }
        if(StringUtils.isBlank(logRecord.dr) || !StrUtils.isInt(logRecord.dr)){
          //dr为空或者不符合整型的，赋予10-60的默认值
          logRecord.dr = (Random.nextInt(50) + 10).toString
        }

        //day hour 处理
        try {
          val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
          logRecord.day = dayHour._1
          logRecord.hour = dayHour._2
        } catch {
          case _: NumberFormatException => {
            val dayHour = ComputeTimeUtils.getDateStrAndHour(System.currentTimeMillis())
            logRecord.day = dayHour._1
            logRecord.hour = dayHour._2
          }
        }
        logRecord
      } else {
        null
      }
    }
    catch {
      case ex: Throwable => {
//        logger.error(s"convert exeception $line", ex)
        null
      }
    }
  }


}
