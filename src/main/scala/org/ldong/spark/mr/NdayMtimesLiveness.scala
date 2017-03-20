package org.ldong.spark.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/3/20 16:17  
  * @version V1.0
  */
object NdayMtimesLiveness extends App {

  //read from the middle output of newly device detail of 30 days

  /**data segment: productid \t deviceid \t opentime
    * 1	0098c54ea52d5c812f3beb50e8c2ae42	2017-03-13 12:21:58
    * 1	0098c54ea52d5c812f3beb50e8c2ae42	2017-03-10 14:52:02
    * 1	0098c54ea52d5c812f3beb50e8c2ae42	2017-03-13 14:05:15
    * 1	0098c54ea52d5c812f3beb50e8c2ae42	2017-03-10 14:14:27
    * 1	0098c54ea52d5c812f3beb50e8c2ae42	2017-03-13 08:43:16
    * 1	0098c54ea52d5c812f3beb50e8c2ae42	2017-03-08 09:55:33
    * 1	0367e9bb85ddc4d03e0a74d1b9f64602	2017-03-09 14:10:09
    * 1	0367e9bb85ddc4d03e0a74d1b9f64602	2017-03-08 11:48:55
    * 1	0367e9bb85ddc4d03e0a74d1b9f64602	2017-03-09 14:35:57
    * 1	0367e9bb85ddc4d03e0a74d1b9f64602	2017-03-08 10:40:48
    * 1	0367e9bb85ddc4d03e0a74d1b9f64602	2017-03-08 13:29:45
    * 1	0367e9bb85ddc4d03e0a74d1b9f64602	2017-03-07 19:17:29
    * 1	0367e9bb85ddc4d03e0a74d1b9f64602	2017-03-07 19:37:54
    */

  val conf = new SparkConf().setMaster("local[1]").setAppName("test N days M times")

  val sc = new SparkContext(conf)

  val filePath = "/jjbox/daily/device/open/middle/startup_newly/part-r-00000"

  //  val textRdd = sc.textFile(args(0))

  val textRdd = sc.textFile(filePath)

  val groupByDeviceRdd = textRdd.map { x =>
    val key = x.split("\t")(0) + "_" + x.split("\t")(1)
    (key, 1)
  }.reduceByKey(_ + _).map { case (key, value) =>
    (value, key)
  }.map { case (times, id_device) =>
    var key = 0
    if (times >= 1 && times < 4) key = 1
    else if (times >= 4 && times < 10) key = 4
    else if (times >= 10) key = 10
    (key, 1)
  }.reduceByKey(_ + _).foreach(println)

  sc.stop()
}
