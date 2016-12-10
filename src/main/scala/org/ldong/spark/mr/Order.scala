package org.ldong.spark.mr

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dongliang on 16/12/10.
  */
object Order extends App {
  val conf = new SparkConf().setAppName("spark sort").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val data = List(1, 4, 2, 9, 3)
  val rdd = sc.parallelize(data)
  val sortResult = rdd.sortBy(x => x).collect
  sortResult.foreach(println)
  sc.stop()
}
