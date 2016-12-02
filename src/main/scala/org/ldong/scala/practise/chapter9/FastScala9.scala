package org.ldong.scala.practise.chapter9

import scala.io.Source
import java.io.PrintWriter

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/2 13:38  
  * @version V1.0
  */
class FastScala9 {
  def reverseLines(filePath: String): Unit = {
    val source = Source.fromFile(filePath, "UTF-8")
    val lines = source.getLines().toArray.reverse
    val printWriter = new PrintWriter(filePath)
    lines.foreach(line => printWriter.write(line + "\n"))
    source.close()
    printWriter.close()
  }

  def reverseLines1(filePath: String): Unit = {
    Source.fromFile(filePath, "UTF-8").getLines().toArray.reverse.foreach(line => new PrintWriter(filePath).write(line + "\n"))
  }

  def repalceTabs(filePath: String): Unit = {
    val source = Source.fromFile(filePath, "UTF-8")
    val lines = source.getLines()
    val newLines = for (line <- lines) yield line.replaceAll("\\t", "    ")
    val printWriter = new PrintWriter(filePath)
    newLines.foreach(line => printWriter.write(line + "\n"))
    source.close()
    printWriter.close()
  }
  def filterChars(filePath: String): Unit = {
    Source.fromFile(filePath).mkString.split("\\s+").filter(word => word.length > 12).foreach(println)
  }

  def calculatePowOfTwo(filePath: String):Unit = {
    val pw = new PrintWriter(filePath)
    for(n <- 0 to 20){
      val t = BigDecimal(2).pow(n)
      pw.write(t.toString())
      pw.write("\t\t")
      pw.write((1/t).toString())
      pw.write("\n")
    }
    pw.close()
  }
}

object FastScala9 extends App {
  val instance = new FastScala9
  instance.reverseLines("C:\\testFiles\\aaaaa.txt")
  instance.repalceTabs("C:\\testFiles\\bbbbb.txt")
  instance.filterChars("C:\\testFiles\\cccc.txt")
  instance.calculatePowOfTwo("C:\\testFiles\\dddd.txt")
}
