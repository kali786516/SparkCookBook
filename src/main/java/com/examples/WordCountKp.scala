package com.examples

/**
 * Created by kalit_000 on 16/09/2015.
 */


import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object WordCountKp
{

  def main(args:Array[String]): Unit =
  {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("Word Count KP").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    //val lines = sc.parallelize("id1,pd1,t1,5.0", "id2,pd2,t2,6.0', 'id1,pd1,t2,7.5', 'id1,pd1,t3,8.1')

    val filename=sc.textFile("C:\\scala_test_folder\\kp.txt")

    //val rdd=sc.parallelize(List("panda 0","pink 3","pirate 3","panda 1","pink 4"))

    //val data = Seq(("a", 3), ("b", 4), ("a", 1))
    //sc.parallelize(data).reduceByKey((x, y) => x + y)
    // Default parallelism
    //
    //sc.parallelize(data).reduceByKey((x, y) => x + y).collect().foreach(println)

    //filename.map(_.).groupByKey().collect().foreach(println)

     val rdd = sc.parallelize(Seq(("a", 2), ("b", 2), ("c", 1), ("D", 4)))
     rdd.map(_.swap).groupByKey().collect().foreach(println)


    //val birds = List("Golden Eagle", "Gyrfalcon", "American Robin",
      //"Mountain BlueBird", "Mountain-Hawk Eagle")

    //val groupedByFirstLetter = birds.groupBy(_.charAt(0))


    // Custom parallelism

    //filename.flatMap(x => x.split(" ")).countByValue()


    //val words=filename.flatMap(x => x.split(" "))

    //words.keys

    //val test=words.map(x => (x,1)).reduceByKey((x,y) => x+y)

    //crap.collect().foreach(print)

    //words.collect().foreach(print)


    //val rdd=filename.map(x => x.split(",")).map(x => x(0)+' '+x(1))

    //val lines=filename.flatMap(x=>x.split(","))
    //val pairs=filename.map(x=> (x.split("\t")(0),x))

    //val pairs2=filename.map(x => x.split("\t"))

    //pairs.collect().foreach(println)
    //pairs2.collect().foreach(println)
    //pairs2.mapValues( x=> (x,1))


    //val pairs2=filename.map(x=> (x.split(" ")(0),x))
    // val b=pairs.zip(pairs)
    //b.collectAsMap().foreach(println)

   // val test3=pairs.keys
    //val test4=pairs.values

    //test4.subtract(test3).collect().foreach(println)


    //test3.collect().foreach(println)
    //test4.collect().foreach(println)

    //pairs.countByValue().foreach(println)


    /*
    val filename=sc.textFile("C:\\scala_test_folder\\kp.txt")

    val fileflat=filename.flatMap(x=>x.split(","))

    val filemap=fileflat.map(x=>(x,1))

    val countofwords=filemap.reduceByKey(_+_)

    countofwords.collect().foreach(println)
     */


    /*
    val result = input.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
       ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
      result.collectAsMap().map(println(_))
    */

  }



}
