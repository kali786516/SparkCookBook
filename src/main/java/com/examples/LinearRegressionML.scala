package com.examples

/**
 * Created by kalit_000 on 03/11/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object LinearRegressionML {

  def main(args:Array[String]): Unit ={
    /*filter out too many logs and warnings*/
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    /*Lets run locally and set app name*/
    val conf= new SparkConf().setMaster("local[50]").setAppName("LinerRegression_APP").set("spark.hadoop.validateOutputSpecs", "false")
    val sc=new SparkContext(conf)

    val points = Array(
      LabeledPoint(1620000,Vectors.dense(2100)),
      LabeledPoint(1690000,Vectors.dense(2300)),
      LabeledPoint(1400000,Vectors.dense(2046)),
      LabeledPoint(2000000,Vectors.dense(4314)),
      LabeledPoint(1060000,Vectors.dense(1244)),
      LabeledPoint(3830000,Vectors.dense(4608)),
      LabeledPoint(1230000,Vectors.dense(2173)),
      LabeledPoint(2400000,Vectors.dense(2750)),
      LabeledPoint(3380000,Vectors.dense(4010)),
      LabeledPoint(1480000,Vectors.dense(1959))
    )
    val pricesRDD=sc.parallelize(points)
    /*
    * 100 iterations
    * very large values of response variables
    * 1.0 fraction of data set
    * weights of different features
    * */
    val model=LinearRegressionWithSGD.train(pricesRDD,100,0.00000006,1.0,Vectors.zeros(1))
    /*Predict the price of house 2,500 sq ft*/
    val prediction=model.predict(Vectors.dense(2500))

    prediction.toString.foreach(println)



  }

}
