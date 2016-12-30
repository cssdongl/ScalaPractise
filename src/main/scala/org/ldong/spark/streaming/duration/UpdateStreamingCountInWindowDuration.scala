package org.ldong.spark.streaming.duration

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/30 14:02  
  * @version V1.0
  */
object UpdateStreamingCountInWindowDuration extends App {
  val updateFunc = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0l)(_ + _)
    val previousCount = state.getOrElse(0l)
    Some(currentCount + previousCount)
  }
  val sparkConf = new SparkConf().setAppName("socket-streaming-wordcount").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  ssc.checkpoint("socket-kafka-wordcount_recent")
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1l))
  //30 seconds slide duration,60 seconds windows duration
  //means caculate the lastest 60 seconds data accumulate every per 30 seconds
  val stateDstream = wordCounts.reduceByKeyAndWindow(_ + _, _ + _, Minutes(1), Seconds(30))

  stateDstream.print()
  stateDstream.repartition(1).saveAsTextFiles("/data/socket-streaming-wordcount.log")

  ssc.start()
  ssc.awaitTermination()
}
