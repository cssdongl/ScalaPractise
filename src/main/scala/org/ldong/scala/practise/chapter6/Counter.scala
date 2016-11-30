package org.ldong.scala.practise.chapter6

/**
  * @author cssdongl@gmail.com
  * @date 2016/11/24 14:33  
  * @version V1.0
  */
class Counter {
  private var value = 0

  def increment(): Unit = {
    if (value < Int.MaxValue) value += 1 else value
  }

  def current() = value
}

object Counter extends App {
  Int.MaxValue
}
