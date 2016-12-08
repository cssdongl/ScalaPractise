package org.ldong.spark.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/8 18:02
  * @version V1.0
  */
object CalculateAvg extends App {
  val sconf = new SparkConf().setAppName("test").setMaster("local[2]")
  val sc = new SparkContext(sconf)
  val foo = sc.parallelize(List(Tuple2("a", 1), Tuple2("a", 3), Tuple2("b", 2), Tuple2("b", 8)))

  //第一个参数:创建一个combiner,是key第一次出现对应的值和出现次数的映射
  //第二个参数:合并value,_1合并的是key对应的value的累加值，_2合并的是key出现的次数,每出现一次加1
  //第三个参数:是对所有分区上的combiner做合并
  var results = foo.combineByKey(
    (v) => (v, 1),
    (accu: (Int, Int), v) => (accu._1 + v, accu._2 + 1),
    (accu1: (Int, Int), accu2: (Int, Int)) => (accu1._1 + accu2._1, accu1._2 + accu2._2)
  ).mapValues(x => (x._1 / x._2)).collect().foreach(println)
  sc.stop()
}
