package org.ldong.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/17 11:35  
  * @version V1.0
  */
object SparkJsonValidateTwo extends App {
  val conf = new SparkConf().setAppName("test device active delay calculate").setMaster("yarn-client")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)
  //delay data
  val delayData = sqlContext.jsonFile("/jjbox/open/2016/12/09/12/*txt")
  delayData.registerTempTable("device_open_delay")
//  val delayFrame = sqlContext.sql("select distinct substr(dod.opentime,1,10),dod.productid,dod.deviceid from device_open_delay dod")
  //normal data
  val normalData = sqlContext.jsonFile("/jjbox/open/2016/12/02/*/*txt")
  normalData.registerTempTable("device_open_normal")
//  val normalFrame = sqlContext.sql("select  substr(don.opentime,1,10),don.productid,don.deviceid from device_open_normal don")

//  val leftJoinResult = sqlContext.sql("select distinct substr(dod.opentime,1,10) as jointime,dod.productid,dod.deviceid,case when dod.country = '' then 'unknown' else dod.country end as country,case when dod.region = '' then 'unknown' else dod.region end as region from device_open_delay dod left join device_open_normal don on  substr(dod.opentime,1,10) = substr(don.opentime,1,10) and dod.productid = don.productid and dod.deviceid = don.deviceid WHERE substr(dod.opentime,1,10) = '2016-12-02'")
  val leftJoinResult = sqlContext.sql("SELECT a.time1, a.productid, a.country, a.region, count(distinct a.deviceid) FROM(SELECT substr(t.opentimeDetail, 1, 10) AS time1, t.productid, t.deviceid, t.country, t.region FROM (SELECT c.productid, c.deviceid, c.country, c.region, max(c.opentime) AS opentimeDetail FROM (SELECT dod.productid, dod.deviceid, dod.opentime, CASE WHEN dod.country = '' THEN 'unknown' ELSE dod.country END AS country, CASE WHEN dod.region = '' THEN 'unknown' ELSE dod.region END AS region FROM device_open_delay dod) c GROUP BY c.productid,c.deviceid,c.country,c.region) t) a LEFT JOIN (SELECT substr(don.opentime, 1, 10) AS time2, don.productid, don.deviceid FROM device_open_normal don WHERE substr(don.opentime,1,10) = substr(don.time,1,10))b ON a.time1 = b.time2 AND a.productid = b.productid AND a.deviceid = b.deviceid GROUP BY a.time1,a.productid,a.country,a.region")

  leftJoinResult.collect().foreach(println)
  sc.stop()
}
