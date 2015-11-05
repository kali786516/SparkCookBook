package com.examples

/**
 * Created by kalit_000 on 27/10/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


object DegreesOfSeperationBFS {

  def main(args:Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("DegreesOfSeperation_BFS")
    val sc = new SparkContext(conf)

    val inputfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\marvelgraph.txt")
    val startCharacterID = 5306
    val targetCharacterID = 14
    val crap: String=""

    def converttoBFS(): Any ={
      val fields=inputfile.map(x => x.split(" "))
      val heroid=fields.map(x => x(0).toInt)
      val connections=""

      //var results = new ListBuffer[Any]
       for (connection <- fields.map(x => if (x.length >= 2 ) x(1).drop(1) else 0.toInt))
         {
           connections+connection
         }
      val color = "WHITE"
      val distance = 9999

      if (heroid == startCharacterID)
        {
          val color ="GRAY"
          val distance=0
          //return (heroid,(connections,distance,color))
          val shit= (heroid,(connections,distance,color))
          val shit2=sc.parallelize(shit.toString())
          shit2.saveAsTextFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\shit.txt")


        }
      }

      crap.foreach(println)

    /*def bfsMap(node:(Int, (Array[Int], Int, Int))): Iterable[(Int, (Array[Int], Int, Int))] = {
      val characterId = node._1
      val data = node._2
      val connections = data._1
      val distance = data._2
      val status = data._3
      var results = new ListBuffer[(Int, (Array[Int], Int, Int))]
      if(status == 1) {
        connections.foreach(connection => {
          if(targetCharacterID == connection) {
            //hitCounter.add(1)
          }
          results += ((connection, (new Array[Int](0), distance + 1, 1)))
        })
      }
      results += ((characterId, (connections, distance, 2)))
      results.toList
    }

    def bfsReduce(data1:(Array[Int], Int, Int), data2:(Array[Int], Int, Int)): (Array[Int], Int, Int) = {
      val edges1 = data1._1
      val edges2 = data2._1
      val distance1 = data1._2
      val distance2 = data2._2
      val status1 = data1._3
      val status2 = data2._3
      val distance = Math.min(distance1, distance2)
      val status = Math.max(status1, status2)
      val edges = if(edges1.length > 0) edges1 else edges2
      (edges, distance, status)
    }*/

  }

}
