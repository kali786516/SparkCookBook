package com.examples

/**
 * Created by kalit_000 on 25/10/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SuperHero {
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SuperHero")
    val sc = new SparkContext(conf)

    val names=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\marvelnames.txt")
    val namemap =names.map(x => x.split("\\ ")).map(x => (x(0),x(1)))

    val graph=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\marvelgraph.txt")
    val graphmap =graph.map(x => x.split(" ")).map(x => (x(0).toInt,(x.length -1).toInt))
    /*
     heroid,countof heroes -1 ex:-((5988,48)
    */


    val totalfirendsBycharacter=graphmap.mapValues(x => (x)).reduceByKey((x,y) => x+y)
    val topsuperhero =totalfirendsBycharacter.sortBy(_._2,false).take(1)
    /*
    * (859,1933)
    */

    def ConvertToMap (value: RDD[(Int,Int)]): RDD[String]  = {
      value.reduceByKey((_, v) => v).values.map(x => x.toString.replace("(","").replace(")",""))
    }


    namemap.lookup(topsuperhero.map(x => x._1).mkString(" ")).foreach(println)


  }


}
