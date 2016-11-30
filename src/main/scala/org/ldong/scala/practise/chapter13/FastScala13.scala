package org.ldong.scala.practise.chapter13

import scala.collection.immutable
import scala.collection.immutable._

/**
  * @author cssdongl@gmail.com
  * @date 2016/11/29 15:46  
  * @version V1.0
  */
class FastScala13 {
  def indexMap(str: String): Map[Char, Set[Int]] = {
    var elemMap = new immutable.HashMap[Char, HashSet[Int]]
    var count: Int = 0
    for (elem <- str) {
      if (elemMap.contains(elem)) {
        val newSet = elemMap(elem) + count
        elemMap += (elem -> newSet)
      }
      else {
        elemMap += (elem -> (new HashSet[Int] + count))
      }
      count = count + 1
    }
    elemMap
  }

  def removeZeroValue(list: List[Int]): Unit = {
    val result = list.filter(_ > 0)
    result.foreach(println)
  }

  def reduceLeftMk(list: List[Int], delimiter: String): String = {
    val result = list.mkString(delimiter)
    println(result)
    result
  }

  def reduceLeftMk1(list: List[String], delimiter: String): String = {
    val result = list.reduceLeft(_ + delimiter + _)
    println(result)
    result
  }

}

object FastScala13 extends App {
  val instance = new FastScala13
  instance.indexMap("DongLiang")
  instance.removeZeroValue(List(1, 2, -1, -2, 3))
  instance.reduceLeftMk(List(1,2,3,4), "_")
  val x = List("xx", "yy", "zz")
  instance.reduceLeftMk1(x, "_")
}