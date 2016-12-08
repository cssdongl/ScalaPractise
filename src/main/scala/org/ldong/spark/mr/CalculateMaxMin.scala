package org.ldong.spark.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/8 12:41
  * @version V1.0
  */
object CalculateMaxMin extends App{
    val conf = new SparkConf().setAppName("test max min").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data  = sc.parallelize(Array(10,8,9,4,3,2,1001))

    val max = data.reduce((a,b) => a max b)
    val min = data.reduce((a,b) => a min b)

    sc.stop()
}
