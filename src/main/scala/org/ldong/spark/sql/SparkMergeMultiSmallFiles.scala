package org.ldong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/17 11:35  
  * @version V1.0
  */
object SparkMergeMultiSmallFiles extends App {
  val conf = new SparkConf().setAppName("test small files performance").setMaster("local[5]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val data2 = sc.wholeTextFiles("/jjbox/open/2017/*/*/*/*txt",30)

  val valuesRdd = data2.values.flatMap(x=> x.split("\n"))

    val jsons = sqlContext.jsonRDD(valuesRdd)

  jsons.registerTempTable("device_open_normal")

  val joinsResult = sqlContext.sql("SELECT DISTINCT a.devicewifimac, a.deviceusid FROM (SELECT DISTINCT don.devicewifimac, don.deviceusid, substr(don.opentime, 1, 10) AS time1 FROM device_open_normal don WHERE don.deviceusid NOT IN ('0123456789abcdef', '0123456789ABCDEF') ) a JOIN (SELECT DISTINCT don.devicewifimac, don.deviceusid, substr(don.opentime, 1, 10) AS time2 FROM device_open_normal don WHERE don.deviceusid = '0123456789abcdef' OR don.deviceusid = '0123456789ABCDEF' ) b ON a.devicewifimac = b.devicewifimac WHERE a.time1 < b.time2")
  joinsResult.toJavaRDD.coalesce(1,true).saveAsTextFile("/test/dongliang/sparkResults")

//  val lines = sc.newAPIHadoopFile("/jjbox/open/2017/*/*/*/*",
//    classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text])
//
//  val jsonObj = lines.values.map {jsonLine =>
//    val data = JSONObject.fromObject(jsonLine)
//    Some(data)
//  }
//
//  val jsons = sc.parallelize(Seq(lines.map(_._2)))
//
//  val tableData = sqlContext.jsonRDD(jsons)


  //  lines.values().filter { case (k, v) => v != null }.collect().foreach(println)

  sc.stop()
}
