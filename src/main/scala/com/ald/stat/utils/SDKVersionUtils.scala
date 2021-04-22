package com.ald.stat.utils

import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zhaofw on 2018/9/7.
 */
object SDKVersionUtils {

  val customSDK = ArrayBuffer("M5.1", "dev1", "D3.99", "B3.88")

  def main(args: Array[String]): Unit = {
    println(isOldSDk("M5.1"))
  }

  /**
   * 判断是否为老版本的SDK
   * 默认认为：7.0.0以前的为老版本
   * 新版本：false
   * 老版本：true
   *
   * @param version
   * @return
   */
  def isOldSDk(version: String): Boolean = {
    val firstChar = version.charAt(0).toString
    val isInt = Pattern.compile("[0-9]").matcher(firstChar).find
    if (isInt) {
      if (firstChar.toInt < 7) return true
    } else {
      for (str <- customSDK)
        if (version == str) return true
    }
    false
  }

  /**
   * 判断version格式是否正常
   * @param version
   * @return
   */
  def properSDK(version: String): Boolean = {
    val isProper: Boolean = Pattern.compile("[0-9].[0-9].[0-9]").matcher(version).find
    return isProper
  }

}
