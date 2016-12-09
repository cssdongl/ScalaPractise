package org.ldong.spark.mr

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/9 11:53  
  * @version V1.0
  */
object CalculateJoin extends App{
  val conf = new SparkConf().setAppName("test join").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val idName = sc.parallelize(Array((1,"zhangsan"),(2,"lisi"),(3,"wangwu")))

  val idAge = sc.parallelize(Array((1,20),(2,25),(4,30)))

  idName.join(idAge).collect().foreach(println)

  idName.rightOuterJoin(idAge).collect().foreach(println)

  idName.leftOuterJoin(idAge).collect().foreach(println)

  sc.stop()
}
