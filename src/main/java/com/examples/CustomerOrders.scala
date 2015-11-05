package com.examples

/**
 * Created by kalit_000 on 24/10/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._


object CustomerOrders {

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("CutomerOrders")
    val sc = new SparkContext(conf)

    /*
    *
    * SQL:-
    *    select customer,sum(cast(amount as Double)) as amount_value from table group by customer
   */

    val file=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\customer-orders.csv")
    val lines =file.map(x => x.split("\\,")).map(x => (x(0),x(2)))
    val test=lines.map(x => (x._1,x._2.toDouble))
    val crap=test.reduceByKey((x,y) => x+y)

    crap.sortBy(_._2,false).take(3).foreach(println)
    crap.sortBy(_._2,true).take(3).foreach(println)

    lines.zipWithIndex.filter(x => x._2 > 0)




  }

}
