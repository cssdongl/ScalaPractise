package org.ldong.spark.machineLearning

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author cssdongl@gmail.com
  * @date 2017/3/6 17:54  
  * @version V1.0
  */
object ExtractFeature {
      val conf = new SparkConf().setAppName("Extract feature").setMaster("local[1]")

      val sc = new SparkContext(conf)

      val rawData = sc.textFile("/home/dongl/testData/spark/machineLearning/ml-100k/u.data")

      rawData.first()

      val rawRatings = rawData.map(_.split("\t")).take(3)

      val ratings = rawRatings.map{case Array(user,movie,rating) =>
        Rating(user.toInt,movie.toInt,rating.toDouble)
      }

      val ranks:Int = 50

      val iterators:Int = 10

      val lambda:Double = 0.01

      val model = ALS.train(ratings,ranks,iterators,lambda)

      sc.stop()


}
