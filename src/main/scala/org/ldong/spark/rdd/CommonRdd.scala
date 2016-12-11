package org.ldong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dongliang on 16/12/11.
  */
object CommonRdd extends App {
  val conf = new SparkConf().setAppName("test commond rdd").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val a = sc.parallelize(List("dog", "tiger", "lion", "cat"))
  val b = a.map(x => (x.length, x))
  val c = b.mapValues("prefix_" + _ + "_suffix").collect().foreach(println)

  sc.stop()
}
