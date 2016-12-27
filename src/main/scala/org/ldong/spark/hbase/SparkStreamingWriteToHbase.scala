package org.ldong.spark.hbase

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ldong.spark.common.PropertiesUtil

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/22 12:44
  * @version V1.0
  * @description spark streaming write data from kafka to hbase
  */
object SparkStreamingWriteToHbase {
  def main(args: Array[String]): Unit = {
    var masterUrl = "yarn-client"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val conf = new SparkConf().setAppName("Write to several tables of Hbase").setMaster(masterUrl)

    val ssc = new StreamingContext(conf, Seconds(5))

    val topics = Set("app_events")

    val brokers = PropertiesUtil.getValue("BROKER_ADDRESS")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    val hbaseTableSuffix = "_clickcounts"

    val hConf = HBaseConfiguration.create()
    val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    hConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)

    val jobConf = new JobConf(hConf, this.getClass)

    val kafkaDStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val appUserClicks = kafkaDStreams.flatMap(rdd => {
      val data = JSONObject.fromObject(rdd._2)
      Some(data)
    }).map{jsonLine =>
        val key = jsonLine.getString("appId") + "_" + jsonLine.getString("uid")
        val value = jsonLine.getString("click_count")
        (key, value)
    }

    val result = appUserClicks.map { item =>
      val rowKey = item._1
      val value = item._2
      convertToHbasePut(rowKey, value, hbaseTableSuffix)
    }

    result.foreachRDD { rdd =>
      rdd.saveAsNewAPIHadoopFile("", classOf[Text], classOf[Text], classOf[MultiTableOutputFormat], jobConf)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def convertToHbasePut(key: String, value: String, tableNameSuffix: String): (ImmutableBytesWritable, Put) = {
    val rowKey = key
    val tableName = rowKey.split("_")(0) + tableNameSuffix
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(value))
    (new ImmutableBytesWritable(Bytes.toBytes(tableName)), put)
  }


}
