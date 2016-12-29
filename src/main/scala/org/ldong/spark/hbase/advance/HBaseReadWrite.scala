/*
 * This example reads a row of time series sensor data
 * calculates the the statistics for the hz data
 * and then writes these statistics to the stats column family
 *
 */

package org.ldong.spark.hbase.advance

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.util.StatCounter
import org.ldong.spark.common.PropertiesUtil

object HBaseReadWrite extends Serializable{

  final val tableName = "sensor"
  final val cfDataBytes = Bytes.toBytes("data")
  final val cfStatsBytes = Bytes.toBytes("stats")

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    conf.set(HConstants.ZOOKEEPER_QUORUM,zookeeper)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_ROW_START, "COHUTTA_3/10/14")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "COHUTTA_3/11/14")
    conf.set(TableInputFormat.SCAN_COLUMNS, "data:psi")

    // Load an RDD of (ImmutableBytesWritable, Result) tuples from the hbase table
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    val rowKeyRDD = hBaseRDD.map(tuple => tuple._1).map(item =>  Bytes.toString(item.get()))
    rowKeyRDD.take(3).foreach(println)

    val resultRDD = hBaseRDD.map(tuple => tuple._2)
    resultRDD.count()

    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()).split(" ")(0), Bytes.toDouble(result.value)))
    keyValueRDD.take(3).foreach(kv => println(kv))

    // group by rowkey , get statistics for column value
    val keyStatsRDD = keyValueRDD.groupByKey().mapValues(list => StatCounter(list))
    keyStatsRDD.take(5).foreach(println)

    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    keyStatsRDD.map { case (k, v) => convertToPut(k, v) }.saveAsHadoopDataset(jobConfig)

  }
  def convertToPut(key: String, stats: StatCounter): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(key))
    p.addColumn(cfStatsBytes, Bytes.toBytes("psimax"), Bytes.toBytes(stats.max))
    p.addColumn(cfStatsBytes, Bytes.toBytes("psimin"), Bytes.toBytes(stats.min))
    p.addColumn(cfStatsBytes, Bytes.toBytes("psimean"), Bytes.toBytes(stats.mean))
    (new ImmutableBytesWritable, p)
  }

}
