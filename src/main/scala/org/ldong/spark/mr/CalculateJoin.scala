package org.ldong.spark.mr

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/9 11:53  
  * @version V1.0
  */
object CalculateJoin extends App {
  val conf = new SparkConf().setAppName("test join").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val idName = sc.parallelize(Array((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))

  val idAge = sc.parallelize(Array((1, 20), (2, 25), (4, 30)))

  idName.join(idAge).collect().foreach(println)

  idName.rightOuterJoin(idAge).collect().foreach(println)

  idName.leftOuterJoin(idAge).collect().foreach(println)

  val schema1 = StructType(Array(StructField("id", DataTypes.IntegerType, nullable = true),
    StructField("name", DataTypes.StringType, nullable = true)))
  val idNameDF = sqlContext.createDataFrame(idName.map(t => Row(t._1, t._2)), schema1)

  val schema2 = StructType(Array(StructField("id", DataTypes.IntegerType, nullable = true),
    StructField("age", DataTypes.IntegerType, nullable = true)))
  val idAgeDF = sqlContext.createDataFrame(idAge.map(t => Row(t._1, t._2)), schema2)

  idNameDF.join(idAgeDF, idNameDF("id") === idAgeDF("id"), "left_outer").collect().foreach(println)

  idNameDF.groupBy("id").count().show()

  idAgeDF.filter(idAgeDF("age") > 23).show()

  idAgeDF.select("age").show()

  sc.stop()
}
