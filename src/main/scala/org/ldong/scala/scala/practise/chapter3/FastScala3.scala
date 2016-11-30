package org.ldong.scala.scala.practise.chapter3

import java.awt.datatransfer._

import scala.collection.mutable._
import scala.math.random

/**
  * @author dongliang@mail.jj.com
  * @date 2016/11/19 13:36  
  * @version V1.0
  * @description the fast scala chapter 3
  */
class FastScala3 {
  def testArrayBuffer(): Unit = {
    val x = ArrayBuffer(1, 2, 3, 4, 5, 6)
    val y = x.count(_ > 1)
    println(y)
  }

  def generateRandomArray(n: Int) = {
    for (i <- 0 until n) yield (random * n).toInt
  }

  def changeNeighborArray(array: Array[Int]): Array[Int] = {
    val t = array.toBuffer
    for (i <- 1 until(t.length, 2); temp = t(i); j <- i - 1 until i) {
      t(i) = t(j)
      t(j) = temp
    }
    t.toArray
  }

  def changeNeighborArray1(array: Array[Int]): Array[Int] = {
    (for (i <- 0 until(array.length, 2)) yield if (i + 1 < array.length) Array(array(i + 1), array(i))
    else
      Array(array(i))).flatten.toArray
  }

  def reorderArray(array: Array[Int]): Array[Int] = {
    val a = array.filter(_ > 0).map(1 * _);
    val b = array.filter(_ <= 0).map(1 * _)
    val c = a.toBuffer
    c ++= b
    c.toArray
  }

  def reorderArray1(array: Array[Int]): Array[Int] = {
    val a = new ArrayBuffer[Int]()
    val b = new ArrayBuffer[Int]()
    array.foreach(x => if (x > 0) a += x else b += x)
    a ++= b
    a.toArray
  }
  def removeRepeat(array: Array[Int]): Array[Int] = {
    val c = array.toBuffer
    c.distinct.toArray
  }

}

object FastScala3 extends App {
  val test = new FastScala3
  test.testArrayBuffer()
  println(test.generateRandomArray(10).mkString(","))
  val array = Array(1, 2, 3, 4, 5)
  test.changeNeighborArray(array).mkString(",").foreach(x => print(x))
  println()
  println(test.changeNeighborArray1(Array(1, 2, 3, 4, 6)).mkString(","))
  println(test.reorderArray(Array(1, 2, -2, -5, 3, 2, 5, -9, 9)).foreach(x => print(x)))
  println(test.reorderArray1(Array(1, 2, -2, -5, 3, 2, 5, -9, 9)).foreach(x => print(x)))
  val array1 = Array(2, 3, 4, 5, 6)
  val average = array1.sum / array1.length
  println(average)
  println(test.removeRepeat(Array(1,2,2,3,3,5,6)).foreach(x => print(x)))
  val flavers = SystemFlavorMap.getDefaultFlavorMap.asInstanceOf[SystemFlavorMap]
  val x = flavers.getNativesForFlavor(DataFlavor.imageFlavor)
  println(x)
}
