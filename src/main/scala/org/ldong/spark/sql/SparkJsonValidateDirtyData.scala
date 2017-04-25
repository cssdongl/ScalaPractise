package org.ldong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/17 11:35  
  * @version V1.0
  */
object SparkJsonValidateDirtyData extends App {
  val conf = new SparkConf().setAppName("test deviceusid change").setMaster("local[5]")
  val sc = new SparkContext(conf)


  val sqlContext = new SQLContext(sc)

  //delay data
  val normalData = sqlContext.jsonFile("/jjbox/open/2017/*/*/*/*txt")
  normalData.registerTempTable("device_open_normal")

  val joinsResult = sqlContext.sql("SELECT DISTINCT a.devicewifimac, a.deviceusid FROM (SELECT DISTINCT don.devicewifimac, don.deviceusid, substr(don.opentime, 1, 10) AS time1 FROM device_open_normal don WHERE don.deviceusid NOT IN ('0123456789abcdef', '0123456789ABCDEF') ) a JOIN (SELECT DISTINCT don.devicewifimac, don.deviceusid, substr(don.opentime, 1, 10) AS time2 FROM device_open_normal don WHERE don.deviceusid = '0123456789abcdef' OR don.deviceusid = '0123456789ABCDEF' ) b ON a.devicewifimac = b.devicewifimac WHERE a.time1 < b.time2")
//  joinsResult.collect().foreach(println)
  //hdfs://littleNameservice/test/dongliang/sparkResults
  joinsResult.toJavaRDD.coalesce(1,true).saveAsTextFile("/test/dongliang/sparkResults")
  sc.stop()
}
