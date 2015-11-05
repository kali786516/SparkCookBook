package com.examples

/**
 * Created by kalit_000 on 24/10/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._


object MaxTemperature {

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("MinTemprature")
    val sc = new SparkContext(conf)

   /*
   *
   * SQL:-
   *    select max(temp),min(temp) from table where weather="TMAX"
  */

    val file=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\1800.csv")
    val lines=file.map(x => x.split("\\,")).map(x => (x(0),x(2),x(3).toFloat*0.1*(9.0/5.0)+32.0))
    val maxtemps=lines.filter(x => x._2.contains("TMAX"))
    val stationtemps=maxtemps.map(x => (x._1,x._3))

    stationtemps.sortBy(_._2,false).take(1).foreach(println) // Max of max Temparatures
    stationtemps.sortBy(_._2,true).take(1).foreach(println) // Min of max Temparatures

  }

}
