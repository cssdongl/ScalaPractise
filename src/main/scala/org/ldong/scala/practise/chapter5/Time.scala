package org.ldong.scala.practise.chapter5

/**
  * @author dongliang@mail.jj.com
  * @date 2016/11/24 14:41  
  * @version V1.0
  */
class Time(val hours: Int, val minutes: Int) {
  def before(other: Time): Boolean = {
    hours < other.hours || (hours == other.hours && minutes < other.minutes)
  }

  override def toString: String = hours + ":" + minutes
}