package org.ldong.spark.hbase

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import org.ldong.spark.common.PropertiesUtil

/**
  * @author cssdongl@gmail.com
  * @date 2017/6/19 11:34  
  * @version V1.0
  */
object HbaseRowKeyScan extends Serializable{

  final val tableName = "device_deviceusidrecorddetails"
  final val cfDataBytes = Bytes.toBytes("info")

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    conf.set(HConstants.ZOOKEEPER_QUORUM,zookeeper)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "info")

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    val rowKeyRDD = hBaseRDD.map(tuple => tuple._1).map(item =>  Bytes.toString(item.get()))
    rowKeyRDD.distinct().collect().foreach(println)

    sc.stop()

  }
}
