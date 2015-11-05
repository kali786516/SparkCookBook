package com.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._

/**
 * Created by kalit_000 on 24/10/2015.
 */
object FakeFriends {

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("FakeFriends")
    val sc = new SparkContext(conf)

    /*
    * Gives average of friends group by age
    * SQL:-
    * with cte as
    * (
    *    select age,count(1) as friends_count,numberoffriends from table group by age,numeroffriends
    * )
    * select age,numberoffriends/friends_count from cte;
    * */

    val file=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\fakefriends.txt")
    val lines =file.map(x => x.split("\\,")).map(x => (x(2).toInt,x(3).toInt))
    val totalsbyage=lines.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val averagebyage=totalsbyage.mapValues(x => (x._1 / x._2))
    averagebyage.foreach(println)

  }

}
