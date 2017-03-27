package org.ldong.spark.mr.topN

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

/**
  * @author cssdongl@gmail.com
  * @date 2017/3/27 13:37  
  * @version V1.0
  */
object MultiColumnTopN {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("multi column topN").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val topN = sc.broadcast(5)

    val textRdd = sc.textFile(args(0))

    val pairRdd = textRdd.map { line =>
      //(weight,line)
      (line.split(",")(1).toInt, line.split(","))
    }
    val partitions = pairRdd.mapPartitions { iter =>
      var sortedMap = SortedMap.empty[Int, Array[String]]
      iter.foreach { pair =>
        sortedMap += pair
        if (sortedMap.size > topN.value){
          sortedMap.takeRight(topN.value)
        }
      }
      //scala sortedMap default sort order is asc,so take the right N values
      sortedMap.takeRight(topN.value).toIterator
    }

//    val partitions = pairRdd.mapPartitions(itr => {
//      var sortedMap = SortedMap.empty[Int, Array[String]]
//      itr.foreach { tuple =>
//      {
//        sortedMap += tuple
//        if (sortedMap.size > topN.value) {
//          sortedMap = sortedMap.takeRight(topN.value)
//        }
//      }
//      }
//      sortedMap.takeRight(topN.value).toIterator
//    })

    val allTopNPartitions = partitions.collect()

    val finalSortedMap = SortedMap.empty[Int,Array[String]].++:(allTopNPartitions)

    val resultTopN =  finalSortedMap.takeRight(topN.value)

    resultTopN.foreach { case (k, v) =>
      println(s"$k \t ${v.asInstanceOf[Array[String]].mkString(",")}")
    }

    sc.stop()

  }
}
