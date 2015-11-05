package com.examples

/**
 * Created by kalit_000 on 24/10/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PopularMovie {

  case class moviesidc(id:Int)
  case class movienames(id:Int,moviename:String)

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("PopularMovies")
    val sc= new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
/*
    import sqlContext.createSchemaRDD

  /*
  *
  * SQL:-
  *  select MovieId,count(1) as numberoftimesmovieswatched from table group by MovieId
  *
 */
    val lines=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\ml-100k\\u.data")
    val movienamesfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\udemy_spark\\ml-100k\\u.item")

    val moviesid=lines.map(x => x.split("\t")).map(x => moviesidc(x(0).toInt))
    moviesid.registerTempTable("movieidtable")

    val movienamesfile2=movienamesfile.map(x => x.split("\\|")).map(x => movienames(x(0).toInt,x(1).toString))
    movienamesfile2.registerTempTable("movienames")

    val result=sqlContext.sql("select a.moviename,count(1) as count_of_movies " +
      "from movienames a join movieidtable b on a.id=b.id group by  a.moviename order by count_of_movies desc limit  3").collect()

    result.take(5).foreach(println)
//1081
/*
    val movieidpair=moviesid.map(x => (x,1))
    val moviecount=movieidpair.reduceByKey((x,y) => x+y)
    val sortedMovies=moviecount.sortBy(_._2,false)

    val crap=sortedMovies.map(x => ((x._1,0),x._2,""))
    //val crap2=shit.map(x => (((x._1,0),0),0,x._2))//.take(2).foreach(println)
    //(1,Toy Story (1995))

    //crap.cartesian(crap2).take(1).foreach(println)
    crap.take(1).foreach(println)
    //crap2.take(1).foreach(println)
    //(((50,0),0),583,)



    //val shit2=shit.map(x => ((x._1.toString,0),0,x._2))
    //val shit3=sortedMovies.map(x => ((x._2),0,""))

    //shit2.take(2).foreach(println)
    //sortedMovies.take(2).foreach(println)



    //((string,int),string) (string,(string,int)))
    //val sortedMoveisWithNames=sortedMovies.map(y => (nameDict.value,y._2))

    //val crap=sortedMovies.map(x => x._2).union(movienamejoined.map(x => (x._1,x._2)))

    //crap.collect().foreach(println)
    //sortedMovies.take(2).foreach(println)
    //movienamejoined.take(5).foreach(println)
    //val results=sortedMoveisWithNames.collect()
    //results.take(2).foreach(println)


    // nameDict.value.take(2).foreach(println)

    //sortedMoveisWithNames.take(3).foreach(println)

   /*
    val results =ratings.countByValue() //(sql :- select count(1) count_of_rows,rattings from table group by rattings)
    val crap=results.toSeq
    val totalsbyage=ratings.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    crap.sortBy(_._2).drop(2).take(5).foreach(println)
   */
*/
* */
  }

}
