package com.examples

/**
 * Created by kalit_000 on 21/09/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object SparkScalaWordCountUser {

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("ScalaWordCount").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

     val comments = sc.parallelize(List(("u1" ,"one two one"), ("u2", "three four three")))
    val wordCounts = comments.flatMap({case (user, comment) =>
      for (word <- comment.split(" ")) yield(((user, word), 1)) }).
      reduceByKey(_ + _)

    val output = wordCounts.
      map({case ((user, word), count) => (user, (word, count))}).
      groupByKey()

    output.collect.foreach(println)










  }

}
