package com.examples

/**
 * Created by kalit_000 on 17/09/2015.
 */

import java.util

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}
import java.util.{Date,Random}
import scala.collection.mutable.ArrayBuffer
import math._


object MemoryFootPrint
{
  def main(args:Array[String]): Unit =
  {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("Memory Foot Print").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    var logger = Logger.getLogger(this.getClass())

    val numbers=List(1, 2, 3, 4, 5)
    val distnumber=sc.parallelize(numbers)

    val countofnumlist=distnumber.count()

    val sumofnumlist=numbers.foldLeft(0)(_+_)

    val averageofnumber=sumofnumlist/countofnumlist

    print("Average of numbers in List:%s".format(averageofnumber))

    //logger.info()
    //val mb = 1024**8

    val mb=1024*16
    val runtime = Runtime.getRuntime
    logger.info("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    logger.info("** Free Memory:  " + runtime.freeMemory / mb)
    logger.info("** Total Memory: " + runtime.totalMemory / mb)
    logger.info("** Max Memory:   " + runtime.maxMemory / mb)


  }
}
