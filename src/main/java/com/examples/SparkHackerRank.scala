package com.examples

/**
 * Created by kalit_000 on 05/11/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SparkHackerRank  {

  def main (args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("RatingsCounter_MovieRatting_InSorted_List")
    val sc = new SparkContext(conf)

    val data=List(2,5,3,4,6,7,9,8)

    data.zipWithIndex.filter(x => x._2 %2 !=0).foreach(println)

  }



}
