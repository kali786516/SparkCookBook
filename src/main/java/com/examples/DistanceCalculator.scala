package com.examples

/**
 * Created by kalit_000 on 16/09/2015.
 */
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}
import math._

object DistanceCalculator
{

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setMaster("local").setAppName("Distance Calculator")
  val sc = new SparkContext(conf)

  //radius of earth
  val R=6372.8

  def distiancefunc(lat1:Double,lon1:Double,lat2:Double,lon2:Double)=
  {
    val DistanceLat = (lat2 - lat1).toRadians
    val DistanceLon = (lon2 - lon1).toRadians

    val a = pow(sin(DistanceLat / 2), 2) + pow(sin(DistanceLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)

    val c = 2 * asin(sqrt(a))

    R * c
  }

    def main(args: Array[String]): Unit=
    {


      var logger = Logger.getLogger(this.getClass())
      if (args.length < 4) {
        logger.error("=> Less number of arguments passed please check")
        System.err.println("Usage: Lat1: 51.153662 Lon1: -0.182063 Lat2:-33.857197 Lon2:151.21514" )
        System.exit(1)
      }

      val Latitude1  =args(0).toDouble
      val Longitude1 =args(1).toDouble
      val Latitude2  =args(2).toDouble
      val Longitude2 =args(3).toDouble

      val output=distiancefunc(Latitude1,Longitude1,Latitude2,Longitude2)

      println("Distance between two desired points in kilometers:%s".format(output))
      println("Distance between two desired points in Miles:%s".format(output*0.62137))


    }
  }


