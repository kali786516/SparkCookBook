package com.examples

/**
 * Created by kalit_000 on 19/09/2015.
 */
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object ChecksumAccumulator {

  def main(args:Array [String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("check casew")
    val sc = new SparkContext(conf)

    //pattern matching

    val x = Seq(true, false)

    for (y <- x) {
      y match {
        case true => print("Got heads")
        case false => print("Got tails")

      }

    }

  }
}
