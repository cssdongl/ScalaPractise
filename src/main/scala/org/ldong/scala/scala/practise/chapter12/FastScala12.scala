package org.ldong.scala.scala.practise.chapter12

import scala.math._
/**
  * @author dongliang@mail.jj.com
  * @date 2016/11/29 16:59  
  * @version V1.0
  */
class FastScala12 {

  def fun = ceil _

  def multiParam(x:Int) = (y:Int) => x * y

}
object FastScala12 extends App{
  val instance = new FastScala12
  println(instance.fun(2.2))
  val fun1 = ceil _
  val result = List(1.2,3.4,5.6).map(fun1)
  result.foreach(println)
  println(instance.multiParam(4)(5))
}
