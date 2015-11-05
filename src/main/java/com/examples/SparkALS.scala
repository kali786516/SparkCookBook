package com.examples

/**
 * Created by kalit_000 on 27/10/2015.
 */

import org.apache.spark.mllib.recommendation.ALS
//import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object SparkAls {

  def main(args:Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("ALS_User_Rating")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\ml-100k\\u.data")

    val ratings = data.map(_.split(" ") match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    //val model = ALS.train(ratings, rank, numIterations, 0.01)

    //ALS.train(ratings, 1, 20, 0.01)
    // Evaluate the model on rating data
    /*val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}

    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rate) => ((user, product), rate)
    }

    val ratesAndPreds = ratings.map{
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map{
      case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
    }.reduce(_ + _)/ratesAndPreds.count

    println("Mean Squared Error = " + MSE)

    // Save and load model
    //model.save(sc, "C:\\Users\\kalit_000\\Desktop\\udemy_spark\\crapmodel.txt")
    //val sameModel = MatrixFactorizationModel.load(sc, "myModelPath")
*/

  }



}
