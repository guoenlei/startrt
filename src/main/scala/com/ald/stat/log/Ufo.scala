package com.ald.stat.log

import scala.beans.BeanProperty

class Ufo {

  @BeanProperty
  var nickName: String = _
  @BeanProperty
  var gender: String = _
  @BeanProperty
  var language: String = _
  @BeanProperty
  var city: String = _
  @BeanProperty
  var province: String = _
  @BeanProperty
  var country: String = _
  @BeanProperty
  var avatarUrl: String = _
  override def toString = s"Ufo($nickName, $gender, $language, $city, $province, $country, $avatarUrl)"
}
