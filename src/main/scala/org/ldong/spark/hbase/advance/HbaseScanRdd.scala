package org.ldong.spark.hbase.advance
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import org.ldong.spark.common.PropertiesUtil
import org.ldong.spark.hbase.tools.HBaseContext


/**
  * @author cssdongl@gmail.com
  * @date 2016/12/29 12:47
  * @version V1.0
  */
object HbaseScanRdd {
  def main(args: Array[String]) {

    val tableName = "sensor"

    val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample " + tableName ).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    conf.set(HConstants.ZOOKEEPER_QUORUM,zookeeper)

    var scan = new Scan()
    scan.setCaching(100)
    scan.setStartRow(Bytes.toBytes("THERMALITO_3/14/14 9:20"))
    scan.setStopRow(Bytes.toBytes("THERMALITO_3/14/14 9:59"))

    val hbaseContext = new HBaseContext(sc, conf)

    var getRdd = hbaseContext.hbaseScanRDD( tableName, scan)
    getRdd.foreach(v => println(Bytes.toString(v._1)))
    getRdd.collect.foreach(v => println(Bytes.toString(v._1)))
  }
}
