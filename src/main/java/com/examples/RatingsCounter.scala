package com.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kalit_000 on 24/10/2015.
 */

object RatingsCounter {

  def main(args:Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("RatingsCounter_MovieRatting_InSorted_List")
    val sc = new SparkContext(conf)

     /*
     * Gives rattings from table with count of rows
     * SQL:-
     * select rattings,count_of_rows from table where rattings != " " order by rattings limit 5
     * */

    val lines=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\ml-100k\\u.data")
    val ratings=lines.flatMap(x => x.split(" ")).map(x => x(2))
    val results =ratings.countByValue() //(sql :- select count(1) count_of_rows,rattings from table group by rattings)

    val crap=results.toSeq
    crap.sortBy(_._1).drop(2).take(5).foreach(println) // (sql:- select rattings,count_of_rows from table where rattings != " " order by rattings limit 5 )


  }

 }
