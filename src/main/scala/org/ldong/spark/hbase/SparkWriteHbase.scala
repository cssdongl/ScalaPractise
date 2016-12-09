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
  * @date 2016/12/9 18:07
  * @version V1.0
  */
object SparkWriteHbase extends App{
  val conf = new SparkConf().setAppName("test").setMaster("local[2]")
  val sc = new SparkContext(conf)

  if(args.length < 1){
    sys.error("Need input file path")
    sys.exit(1)
  }

  /**
  0015A49A8F2A60DACEE0160545C58F94,1234
  0152C9666B5F3426DDDB5FB74BDBCE4F,4366
  0160D90AC268AEB595208E8448C7F8B8,6577
  0225A39EB29BDB582CA58BE86791ACBC,1234
  02462ACDF7232C49890B07D63B50C5E1,4366
    */

  val data = sc.textFile(args(0))

  data.foreach(println)

  val hConf = HBaseConfiguration.create()
  val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
  hConf.set(HConstants.ZOOKEEPER_QUORUM,zookeeper)

  val jobConf = new JobConf(hConf,this.getClass)
  jobConf.set(TableOutputFormat.OUTPUT_TABLE,"spark_hbase")
  jobConf.setOutputFormat(classOf[TableOutputFormat])

  val result = data.map { item =>
    //here seems the \t as split char does not work after tried several times
    val Array(key,value) = item.split(",")
    val rowKey = key.reverse
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("column1"),Bytes.toBytes(value))
    (new ImmutableBytesWritable(),put)
  }
  result.saveAsHadoopDataset(jobConf)
  sc.stop()
}
