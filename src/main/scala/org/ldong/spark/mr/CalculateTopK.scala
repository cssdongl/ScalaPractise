package org.ldong.spark.mr

import collection.mutable._
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author cssdongl@gmail.com
  * @date 2016/12/20 9:48  
  * @version V1.0
  */
object CalculateTopK extends App{
  val conf = new SparkConf().setAppName("calculate topK(biggest)").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val rdd = sc.textFile(args(0))

  //method one
  val middleResult = rdd.flatMap(line => line.split("\\s")).map(word =>(word,1)).reduceByKey(_+_)

  val sortResult = middleResult.map{case(key,value) => (value,key)}.sortByKey(true,1)

  val topN = 3

  val topNResult = sortResult.top(topN).map{
    case(key,value) => (value,key)
  }.foreach(println)

  //method two

  val mapPartitionResult = middleResult.mapPartitions{iterator =>
    while(iterator.hasNext){

    }
    getHeap().iterator
  }

  def pushToHeap(iter:(String,Int)): Unit ={
    val stack =  Stack[(String,Int)]()
    stack.push(iter)
  }

  def getHeap():Array[(String,Int)] = {
    val array = Array[(String,Int)]()
    array
  }


}
