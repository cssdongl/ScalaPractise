package org.ldong.spark.rdd

import java.util


/**
  * @author cssdongl@gmail.com
  * @date 2017/4/13 18:15  
  * @version V1.0
  */
object CalculateIndex extends App {
//
//  val str = "google"
//
//  var sortedMap = new util.TreeMap[Integer, Char]()

  for (x <- str) {
    var strBuffer = str.toBuffer
    val index = strBuffer.indexOf(x)
    //    val sb = new StringBuilder
    //    var i = 0
    //    strBuffer.foreach{character =>
    //      if(i != index){
    //        sb.append(character)
    //      }
    //      i = i+1
    //    }
    val otherPart = str.drop(str.indexOf(x) + 1)
    if (!otherPart.toString.contains(x)) {
      sortedMap.put(index, x)
    }

  }

  val str = "google"

  var sortedMap = new util.TreeMap[Integer, Char]()

  "google".foreach{character =>
    if (!str.drop(str.indexOf(character) + 1).toString.contains(character)) {
      println("the first char index not repeat is:" + str.indexOf(character))
    }
  }
  // sortedMap.put(str.indexOf(character), character)
  println("the first char not repeat is:" + sortedMap.get(sortedMap.firstKey()))
}
