package org.ldong.spark.hbase.advance

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import org.ldong.spark.common.PropertiesUtil
import org.ldong.spark.hbase.tools.HBaseContext

import scala.collection.JavaConversions


/**
  * @author cssdongl@gmail.com
  * @date 2016/12/29 12:47
  * @version V1.0
  */
object HbaseScanRdd {
  def main(args: Array[String]) {

    val tableName = "sensor"

    val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample " + tableName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)

    var scan = new Scan()
    scan.setCaching(100)
    scan.setStartRow(Bytes.toBytes("THERMALITO_3/14/14 9:20"))
    scan.setStopRow(Bytes.toBytes("THERMALITO_3/14/14 9:59"))

    val hbaseContext = new HBaseContext(sc, conf)

    var hbaseRdd = hbaseContext.hbaseScanRDD(tableName, scan)


    val rowKeyRdd = hbaseRdd.map(tuple => tuple._1)
    rowKeyRdd.foreach(key => println(Bytes.toString(key)))

    //method one
    //    val resultRdd = getRdd.map(tuple => tuple._2).foreach { arraysList =>
    //      var columnArrayNumPerKey = 0
    //      while (columnArrayNumPerKey < arraysList.size()) {
    //        val array = arraysList.get(columnArrayNumPerKey)
    //        val cf = Bytes.toString(array._1)
    //        val col = Bytes.toString(array._2)
    //        val value = Bytes.toString(array._3)
    //        println(cf + "_" + col + "_" + value)
    //        columnArrayNumPerKey += 1
    //        println("column array number for per row key is " + columnArrayNumPerKey)
    //      }
    //    }

    //method two(just convert java list to scala and use <- to interator)
    //    val resultRdd1 = getRdd.map(tuple => tuple._2).foreach { arraysList =>
    //      var columnArrayNumPerKey = 0
    //      val scalaArraysList = JavaConversions.asScalaBuffer(arraysList)
    //      for(array <- scalaArraysList){
    //        val cf = Bytes.toString(array._1)
    //        val col = Bytes.toString(array._2)
    //        val value = Bytes.toString(array._3)
    //        println(cf + "_" + col + "_" + value)
    //        columnArrayNumPerKey += 1
    //        println("column array number for per row key  is " + columnArrayNumPerKey)
    //      }
    //    }

    val resultRdd2 = hbaseRdd.map(tuple => tuple._2).foreach { arraysList =>
      val scalaArraysList = JavaConversions.asScalaBuffer(arraysList)
      scalaArraysList.foreach { array =>
        val Array(cf,col,value) = Array(Bytes.toString(array._1),
          Bytes.toString(array._2),Bytes.toString(array._3))
        println(cf + "_" + col + "_" + value)
      }
    }


    val resultRdd3 = hbaseRdd.map(tuple => tuple._2).map { arraysList =>
      val scalaArraysList = JavaConversions.asScalaBuffer(arraysList)
      scalaArraysList.foreach { array =>
        val Array(cf,col,value) = Array(Bytes.toString(array._1),
          Bytes.toString(array._2),Bytes.toString(array._3))
        val key = cf + "_" + col
        (key,value)
      }
    }
    sc.stop()
  }
}
