package org.ldong.spark.mr.topN

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/3/27 11:39  
  * @version V1.0
  */
object SingleColumnTopN {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("single column topN")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val recordRdd = sc.textFile(args(0)).map(x => x.toInt)

    val topN = recordRdd.top(5).foreach(println)

    sc.stop()

  }

}
