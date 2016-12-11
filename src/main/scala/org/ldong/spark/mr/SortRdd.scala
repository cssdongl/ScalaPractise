package org.ldong.spark.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dongliang on 16/12/10.
  */
object SortRdd extends App {
  val conf = new SparkConf().setAppName("spark sort").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val data = List(1, 4, 2, 9, 3)
  val rdd = sc.parallelize(data)
  //by default the sort is ascend order
  val sortResult = rdd.sortBy(x => x).collect
  sortResult.foreach(println)

  val sortResult1 = rdd.sortBy(x => x,false).collect
  sortResult1.foreach(println)

  //can change the partition size while sort
  val sortResult2 = rdd.sortBy(x => x,false,1).collect
  sortResult2.foreach(println)

  sc.stop()
}
