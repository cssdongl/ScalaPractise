package org.ldong.scala.practise.chapter1

import scala.math.BigInt._
import scala.util.Random;

/**
  * @author dongliang@mail.jj.com
  * @date 2016/11/19 10:00
  * @version V1.0
  * @Description the first chapter for study scala
  */
class FastScala1 {

  def showMaxOne(): Unit = {
    //max here is for compare the big one
    println(10 max 2)
  }

  def calcuBigInt() = {
    val y = BigInt(2) pow 1024
    println(y)
  }

  def showRandomPrime(): Unit = {
    val y = probablePrime(100, new Random())
    println(y)
  }

  def convertRadix(): Unit = {
    val res = BigInt(new Random().nextInt()).toString(36)
    println(res)
  }

  def getFirstLastChar(xx: String): Unit = {
    val firstChar: Char = xx.head
    val lastChar: Char = xx.last
    println(firstChar)
    println(lastChar)
  }
  def testStringAPI(xx:String): Unit ={
    println(xx.take(1))
    println(xx.drop(2))
    println(xx.takeRight(2))
    println(xx.dropRight(3))
  }
}

object FastScala1 extends App {
  val test = new FastScala1
  test.showMaxOne()
  test.calcuBigInt()
  test.showRandomPrime()
  test.convertRadix()
  test.getFirstLastChar("DongLiang")
  test.testStringAPI("DongLiang")
}

