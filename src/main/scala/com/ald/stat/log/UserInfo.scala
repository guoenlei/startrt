package com.ald.stat.log

import scala.beans.BeanProperty

class UserInfo {

  @BeanProperty
  var userInfo: String = _
  override def toString = s"Ufo($userInfo)"
}
