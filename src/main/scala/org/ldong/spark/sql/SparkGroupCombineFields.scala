package org.ldong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/4/28 9:48  
  * @version V1.0
  */
object SparkGroupCombineFields extends App {

  val conf = new SparkConf().setAppName("test group mapping one to many").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val lines = sc.textFile("/jjbox/open/2017/*/*/*/*").coalesce(100, false)

  lines.take(5).foreach(println)

  val jsons = sqlContext.jsonRDD(lines)

  jsons.registerTempTable("device_open_normal")

  val joinsResult = sqlContext.sql("select distinct don.deviceusid, don.devicewifimac from device_open_normal don where don.deviceusid <> '0123456789abcdef' and substr(don.opentime, 1, 10) > '2017-01-15'")

  val combineFieldsRdd = joinsResult.rdd.map { line =>
    val leftPart = line.toString().split(",")(0).replace("[", "")
    val rightPart = line.toString().split(",")(1).replace("]", "")
    (leftPart, rightPart)
  }.reduceByKey(_ + "_" + _).filter { case (key, value) =>
    value.split("_").length > 1
  }.coalesce(1, true).saveAsTextFile("/test/dongliang/sparkResults")

  sc.stop()

}
