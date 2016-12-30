package org.ldong.spark.streaming.duration

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/30 12:05
  * @version V1.0
  */
object UpdateStreamingCountHistory extends App {
  val sparkConf = new SparkConf().setAppName("socket-streaming-wordcount").setMaster("local[3]")
  //batch duration is 10 seconds
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  //the accumulate function
  val updateFunc = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0l)(_ + _)
    val previousCount = state.getOrElse(0l)
    Some(currentCount + previousCount)
  }
  ssc.checkpoint("/spark/checkpoint")
  val lines = ssc.socketTextStream("datanode01.jj.wl", 9999)
  val wordCounts = lines.flatMap(_.split(" ")).map(x => (x, 1l))
  //merge current and all history count
  val history_stream = wordCounts.updateStateByKey[Long](updateFunc)

  history_stream.print()
  history_stream.repartition(1).saveAsTextFiles("/spark/socket_wordcount_history.")

  ssc.start()
  ssc.awaitTermination()
}
