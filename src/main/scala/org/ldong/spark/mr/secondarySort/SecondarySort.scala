package org.ldong.spark.mr.secondarySort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/3/23 10:14  
  * @version V1.0
  * @description the secondary sort implement by scala
  */
object SecondarySort extends App {

  val conf = new SparkConf().setAppName("secondary sort").setMaster("local[1]")

  val sc = new SparkContext(conf)

  val filePath = args(0)

  /** sample data:Year,Month,Day,Temperature
    * 2000,12,04,10
    * 2000,11,01,20
    * 2000,12,02,-20
    * 2000,11,07,30
    * 2000,11,24,-40
    * 2000,12,21,30
    * 2012,12,21,30
    * 2012,12,22,-20
    * 2012,12,23,60
    * 2012,12,24,70
    * 2012,12,25,10
    * 2013,01,22,80
    */
  val valueToKey = sc.textFile(filePath).map(x => {
    val line = x.split(",")
    ((line(0) + "-" + line(1), line(3).toInt), line(3).toInt)
  })

  implicit def tupleOrderingDesc = new Ordering[Tuple2[String, Int]] {
    override def compare(x: Tuple2[String, Int], y: Tuple2[String, Int]): Int = {
      if (y._1.compare(x._1) == 0) y._2.compare(x._2)
      else y._1.compare(x._1)
    }
  }

  val sorted = valueToKey.repartitionAndSortWithinPartitions(new CustomPartitioner(2))

  val result = sorted.map {
    case (k, v) => (k._1, v)
  }.groupByKey().map { tuple =>
    val yearMonthKey = tuple._1
    val valuesBuffer = tuple._2
    val sb = new StringBuilder
    for (x <- valuesBuffer) {
      if(valuesBuffer.last != x){
        sb.append(x)
        sb.append(",")
      }else{
        sb.append(x)
      }

    }
    (yearMonthKey+":",sb.toString())
  }.sortByKey()


  val count = result.count()

  println (s"the YearMonth line count is $count")

  result.collect.foreach(println)

  val result1 = valueToKey.map{case(k,v) => (k._1,v)}.
    groupByKey.map(x => (x._1, x._2.toList.sortWith(_ > _))).collect.foreach(println)


  sc.stop()

}
