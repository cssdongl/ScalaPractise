package org.ldong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/3/29 10:06  
  * @version V1.0
  */
object SparkJsonValidateUniqueDevice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test unique device").setMaster("yarn-client")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val sqlData = sqlContext.jsonFile("/jjbox/open/2017/03/27/*/*.txt")

    sqlData.registerTempTable("deviceusiddetail")

    //self join

    val result = sqlContext.sql("SELECT DISTINCT a.deviceusid, a.deviceid, a.latesttime, b.otaversion, b.devicewifimac\n\t, b.devicecontrolmac, b.operator, b.clientip\nFROM (SELECT dul.deviceusid, dul.deviceid, MAX(dul.opentime) AS latesttime\n\tFROM deviceusiddetail dul\n\tWHERE dul.deviceusid <> '0123456789abcdef'\n\tGROUP BY dul.deviceusid, dul.deviceid\n\t) a\n\tLEFT JOIN (SELECT dul.deviceusid, dul.otaversion, dul.devicewifimac, dul.devicecontrolmac, dul.operator\n\t\t\t, dul.clientip, dul.opentime\n\t\tFROM deviceusiddetail dul\n\t\t) b ON a.deviceusid = b.deviceusid\n\t\tAND a.latesttime = b.opentime")

    result.collect().foreach(println)

    sc.stop()
  }
}
