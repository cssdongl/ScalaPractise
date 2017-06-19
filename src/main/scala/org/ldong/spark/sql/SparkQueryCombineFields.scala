package org.ldong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/4/28 9:48  
  * @version V1.0
  */
object SparkQueryCombineFields extends App{

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

  val joinsResult = sqlContext.sql("select don.deviceusid, concat_ws('_',collect_set(don.devicewifimac)) as deviceWifiMacs from device_open_normal don where don.deviceusid <> '0123456789abcdef' and substr(don.opentime, 1, 10) > '2017-01-15' group by don.deviceusid")

  joinsResult.rdd.coalesce(1,false).saveAsTextFile("/test/dongliang/sparkResults")


  sc.stop()

}
