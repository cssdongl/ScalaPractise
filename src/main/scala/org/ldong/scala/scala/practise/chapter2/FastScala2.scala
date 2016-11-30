package org.ldong.scala.scala.practise.chapter2

import java.net.URL

import com.sun.org.apache.xerces.internal.util.URI.MalformedURIException

/**
  * @author dongliang@mail.jj.com
  * @date 2016/11/19 10:56
  * @version V1.0
  * @description the fast scala chapter 2
  */
class FastScala2 {
  def ifelseTest(x: Int): Unit = {
    println(if (x > 4) x else 4)
  }

  def catchTest(url: String): Unit = {
    try {
      new URL("http://sfsdf.dif")
    } catch {
      case _: MalformedURIException => println("Bad ur" + url)
      case ex: Exception => ex.printStackTrace()
    }
  }

  def assginValidValue(): Unit = {
    var y = 0
    var x = {}
    x = y = 7
    println("x is " + x)
    println("y is " + y)
  }

  def emptyBlock() {}

  def foreachTest(number: Int): Unit = {
    for (x <- 1 to number reverse) {
      print(x + " ")
    }
    println()
  }

  def unicodeMulti(string: String): Unit = {
    var result: Long = 1
    for (x <- string) {
      result = result * x.toLong
    }
    println(result)
  }

  def product(str: String): Unit = {
    var t: Long = 1
    str.foreach(t *= _.toLong)
    println(t)
  }

}

object FastScala2 extends App {
  printf("string is %s", "string")
  println()
  val test = new FastScala2
  test.ifelseTest(7)
  test.emptyBlock()
  test.assginValidValue()
  test.foreachTest(10)
  test.unicodeMulti("DongLiang")
  test.product("Dong")
}
