package org.ldong.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/13 14:19  
  * @version V1.0
  */
object SparkJsonValidate extends App{
  val conf = new SparkConf().setAppName("test device open delay calculate").setMaster("yarn-client")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val data = sqlContext.jsonFile("/jjbox/open/2016/12/09/12/*txt")
  data.registerTempTable("device_open_clock12")
  val result = sqlContext.sql("select substr(do.opentime,1,10),do.productid,do.network,count(*) from device_open_clock12 do where substr(do.opentime,1,10) <> '2016-12-09' group by substr(do.opentime,1,10),do.productid,do.network")
  result.collect().foreach(println)
  sc.stop()
}
