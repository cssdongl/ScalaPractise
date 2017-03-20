package org.ldong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/17 11:35  
  * @version V1.0
  */
object SparkJsonValidateNdaysMtimes extends App {
  val conf = new SparkConf().setAppName("test device liveness").setMaster("yarn-client")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)
  //delay data
  val normalData = sqlContext.jsonFile("/jjbox/open/2017/*/*/*/*txt")
  normalData.registerTempTable("device_open_normal")

  val leftJoinResult = sqlContext.sql("SELECT b.region, COUNT(b.deviceid) FROM (SELECT a.deviceid, CASE WHEN a.launchTimes >= 1 AND a.launchTimes < 4 THEN 2 WHEN a.launchTimes >= 4 AND a.launchTimes < 10 THEN 4 WHEN a.launchTimes >= 10 THEN 10 ELSE 1 END AS region FROM (SELECT don.deviceid, COUNT(*) AS launchTimes FROM device_open_normal don WHERE substr(don.opentime, 1, 10) BETWEEN '2017-02-11' AND '2017-03-12' GROUP BY don.deviceid ) a ) b GROUP BY b.region")
  leftJoinResult.collect().foreach(println)
  sc.stop()
}
