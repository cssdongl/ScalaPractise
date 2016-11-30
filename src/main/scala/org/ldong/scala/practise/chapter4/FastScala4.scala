package org.ldong.scala.practise.chapter4

import java.util.Scanner

import scala.collection.immutable.SortedMap
import scala.collection.{immutable, mutable}
import scala.io.Source

/**
  * @author cssdongl@gmail.com
  * @ate 2016/11/19 15:00
  * @version V1.0
  * @description the fast scala chapter 4
  */
class FastScala4 {
  def constuctMap(): Unit = {
    val enquipmentMap = Map(("blade" -> 45), ("archor" -> 40))
    val result = for ((k, v) <- enquipmentMap) yield (k, v * 0.9)
    for ((k, v) <- result) {
      println(v)
    }
  }

  def scanAndWordCount(): Unit = {
    val path = "C:\\scalaFile.txt"
    val scanner = new Scanner(new java.io.File(path))
    val wordMap = new mutable.HashMap[String, Int]()

    while (scanner.hasNext) {
      val word = scanner.next()
      if (!wordMap.contains(word)) wordMap(word) = 1 else wordMap(word) += 1
    }
    for ((k, v) <- wordMap) println("k is " + k + " v is " + v)
  }

  def scanAndWordCount1(): Unit = {
    val path = "C:\\scalaFile.txt"
    val scanner = new Scanner(new java.io.File(path))
    var wordMap = new immutable.HashMap[String, Int]

    while (scanner.hasNext) {
      val word = scanner.next()
      if (!wordMap.contains(word)) {
        wordMap += (word -> 1)
      } else {
        wordMap += (word -> (wordMap(word) + 1))
      }
    }
    for ((k, v) <- wordMap) println("k is " + k + " v is " + v)
  }

  def scanAndWordCount2(): Unit = {
    val path = "C:\\scalaFile.txt"
    val source = Source.fromFile(path).mkString

    val tokens = source.split("\\s+")

    var map = SortedMap[String, Int]()

    for (key <- tokens) {
      map += (key -> (map.getOrElse(key, 0) + 1))
    }

    println(map.mkString(","))
  }

  def minmax(value: Array[Int]): Unit = {
//    val minArray = Array(value.min)
//    val maxArray = Array(value.max)
//    val arr = minArray.zip(maxArray)
//    for ((x, y) <- arr) print("min is " + x + " max is " + y)
    (value.min,value.max)
  }
  def minmax1(value: Array[Int],middle:Int): Unit = {
    (value.count(_ > middle),value.count(_ < middle))
  }
}

object FastScala4 extends App {
  val test = new FastScala4
  test.constuctMap()
  test.scanAndWordCount()
  test.scanAndWordCount1()
  test.scanAndWordCount2()
  test.minmax(Array(12,3,4,5))
}
