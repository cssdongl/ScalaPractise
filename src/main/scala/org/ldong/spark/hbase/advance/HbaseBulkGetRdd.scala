/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ldong.spark.hbase.advance

import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import org.ldong.spark.common.PropertiesUtil
import org.ldong.spark.hbase.tools.HBaseContext

object HbaseBulkGetRdd {
  def main(args: Array[String]) {

    val tableName = "sensor"

    val sparkConf = new SparkConf().setAppName("HBaseBulkGetExample " + tableName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //here construct the rowkey bytes RDD for bulkGet
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("THERMALITO_3/14/14 9:50"))
    ))

    val conf = HBaseConfiguration.create()
    val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)

    val hbaseContext = new HBaseContext(sc, conf)

    val getRdd = hbaseContext.bulkGet[Array[Byte], String](
      tableName,
      2,
      rdd,
      record => {
        System.out.println("making Get")
        new Get(record)
      },
      (result: Result) => {

        val it = result.list().iterator()
        val b = new StringBuilder

        b.append(Bytes.toString(result.getRow()) + ":")

        while (it.hasNext()) {
          val kv = it.next()
          b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toLong(kv.getValue()) + ")")
        }
        b.toString
      })

    getRdd.collect.foreach(v => System.out.println(v))

  }
}