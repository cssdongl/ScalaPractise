package org.ldong.spark.etl

import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * @author cssdongl@gmail.com
  * @date 2017/1/3 18:02
  * @version V1.0
  */
object SparkUnzipEtl extends App {
  val sparkConf = new SparkConf().setAppName("spark unzip etl").setMaster("yarn-client")

  val sc = new SparkContext(sparkConf)


  sc.binaryFiles("/smallfile/sparktest/zipdir")

    .flatMap((file: (String, PortableDataStream)) => {
      val zipStream = new ZipInputStream(file._2.open)
      val entry = zipStream.getNextEntry
      val iter = Source.fromInputStream(zipStream).getLines
      iter.next
      iter
    }).saveAsTextFile("/smallfile/sparktest/mergeTable_csv/result.csv")

  sc.stop()
}
