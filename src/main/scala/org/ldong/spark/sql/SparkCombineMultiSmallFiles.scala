package org.ldong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/4/28 9:48  
  * @version V1.0
  */
object SparkCombineMultiSmallFiles extends App{

  val conf = new SparkConf().setAppName("test small files performance").setMaster("yarn-client")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  //methond two
//  val lines1 = sc.newAPIHadoopFile("/jjbox/open/2017/*/*/*/*", classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text])

  val lines = sc.textFile("/jjbox/open/2017/*/*/*/*").coalesce(100, false)

//  val repartitionLines = lines.repartition(20)

  lines.take(5).foreach(println)

  val jsons = sqlContext.jsonRDD(lines)

  jsons.registerTempTable("device_open_normal")

  val joinsResult = sqlContext.sql("SELECT DISTINCT a.devicewifimac, a.deviceusid FROM (SELECT DISTINCT don.devicewifimac, don.deviceusid, substr(don.opentime, 1, 10) AS time1 FROM device_open_normal don WHERE don.deviceusid NOT IN ('0123456789abcdef', '0123456789ABCDEF') ) a JOIN (SELECT DISTINCT don.devicewifimac, don.deviceusid, substr(don.opentime, 1, 10) AS time2 FROM device_open_normal don WHERE don.deviceusid = '0123456789abcdef' OR don.deviceusid = '0123456789ABCDEF' ) b ON a.devicewifimac = b.devicewifimac WHERE a.time1 < b.time2")

  joinsResult.toJavaRDD.coalesce(1, true).saveAsTextFile("/test/dongliang/sparkResults")

  sc.stop()

}
