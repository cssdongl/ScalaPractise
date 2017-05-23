package org.ldong.spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.ldong.spark.common.PropertiesUtil

/**
  * @author cssdongl@gmail.com
  * @date 2017/5/23 14:11  
  * @version V1.0
  */
object SparkWriteUrlDataToHbase extends App{

  val conf = new SparkConf().setAppName("handle url data").setMaster("local[2]")

  val sc = new SparkContext(conf)

  val textRdd = sc.textFile(args(0))

  println("the rdd size is:~~~"+textRdd.count())

  val hConf = HBaseConfiguration.create()
  val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
  hConf.set(HConstants.ZOOKEEPER_QUORUM,zookeeper)

  val jobConf = new JobConf(hConf,this.getClass)
  jobConf.set(TableOutputFormat.OUTPUT_TABLE,"phoenix_hbase")
  jobConf.setOutputFormat(classOf[TableOutputFormat])

  // time \t userid \t keyword \t ranking \t number \t url

  val hbaseItem = textRdd.map{line =>

    val keyword = line.split("\t")(2)

    val url = line.split("\t").last

    val userid = line.split("\t")(1)

    val put:Put = new Put(Bytes.toBytes(url))

    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("id"),Bytes.toBytes(userid))

    (new ImmutableBytesWritable(),put)
  }

  hbaseItem.saveAsHadoopDataset(jobConf)

  sc.stop()
}
