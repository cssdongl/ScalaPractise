package org.ldong.spark.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/15 11:49  
  * @version V1.0
  */
object ColumnValueSum extends App {
  /**
    * ID,Name,ADDRESS,AGE
    * 001,zhangsan,chaoyang,20
    * 002,zhangsa,chaoyang,27
    * 003,zhangjie,chaoyang,35
    * 004,lisi,haidian,24
    * 005,lier,haidian,40
    * 006,wangwu,chaoyang,90
    * 007,wangchao,haidian,80
    */
  val conf = new SparkConf().setAppName("test column value sum and avg").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val textRdd = sc.textFile(args(0))

  //be careful the toInt here is necessary ,if no cast ,then it will be age string append
  val addressAgeMap = textRdd.map(x => (x.split(",")(2), x.split(",")(3).toInt))

  val sumAgeResult = addressAgeMap.reduceByKey(_ + _).collect().foreach(println)

  val avgAgeResult = addressAgeMap.combineByKey(
    (v) => (v, 1),
    (accu: (Int, Int), v) => (accu._1 + v, accu._2 + 1),
    (accu1: (Int, Int), accu2: (Int, Int)) => (accu1._1 + accu2._1, accu1._2 + accu2._2)
  ).mapValues(x => (x._1 / x._2).toDouble).collect().foreach(println)

  sc.stop()

  println("Sum and Avg calculate successfuly")

}
