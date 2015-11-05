package com.examples

/**
 * Created by kalit_000 on 09/10/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}

object CitiMarket
{
  def main(args: Array[String]): Unit =
  {

    /*filter out too many logs and warnings*/
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    /*Lets run locally and set app name*/
    val conf= new SparkConf().setMaster("local[50]").setAppName("CitiSimpleMarketData").set("spark.hadoop.validateOutputSpecs", "false")
    val sc=new SparkContext(conf)

    def error(message: String)=throw new Error(message)

    val statusfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\mkdata\\status.txt",16)
    val luldfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\mkdata\\luld.txt",16)
    val quotefile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\mkdata\\quote_2.txt",16)


    /*CREATE STATUS DATA SET*/
    val StatusRDD=statusfile.map(x => x.split("\\|")).filter(line => line(0).contains("1013")).map(x => (x(5)+x(4),x(5),x(4),x(1),x(6),x(7),x(8)))
    println(StatusRDD.toDebugString)
    val StatusRDDData =StatusRDD.collect()
    val StatusRDDDataSet=sc.parallelize(StatusRDDData)
    println("Count of STATUS Data set %s".format(StatusRDDDataSet.count()))
    //StatusRDDDataSet.collect().foreach(println)

    /*CREATE LULD DATA SET*/
    val LuldRDD=luldfile.map(x => x.split("\\|")).filter(line => line(0).contains("1041")).map(x => (x(5)+x(4),x(5),x(4),x(1),x(6),x(7),x(8),x(9)))
    println (LuldRDD.toDebugString)
    val LuldRDDData =LuldRDD.collect()
    val LuldRDDDataSet=sc.parallelize(LuldRDDData)
    println("Count of Luld Data set %s".format(LuldRDDDataSet.count()))
    //LuldRDDDataSet.collect().foreach(println)



    val QuoteRDD=quotefile.map(x => x.split("\\|")).
      filter(line => line(0).contains("1017")).
      map(x => ((x(5)+x(4)) , (x(5),x(4),x(1) ,
      if (x(15) =="B")
        (
          {if (x(25) == "") x(9)   else x(25)},
          {if (x(37) == "") x(11)  else x(37)}
        )
      else if (x(15) =="C" )
        (
           {if (x(24) == "") (x(9))  else x(24)},
           {if (x(30) == "") (x(11)) else x(30)}
        )
      else if (x(15) =="A")
           {(x(9),x(11))}
        )))


    val QuoteHashMap=QuoteRDD.collect().toMap
    val test=QuoteHashMap.values.toSeq
    val test2=sc.parallelize(test.map(x => x.toString.replace("(","").replace(")","")))
    test2.saveAsTextFile("C:\\Users\\kalit_000\\Desktop\\mkdata\\test.txt")
    test2.collect().foreach(println)

    //QuoteHashMap.mapValues(x => x.toString().replace("(","").replace(")",""))


    /*FLUSH JVM CACHE MEMORY*/
    println("FLUSH JVM CACHE MEMEORY FOR STATUS DATA SET ....")
    statusfile.unpersist()
    luldfile.unpersist()
    quotefile.unpersist()
    StatusRDD.unpersist()
    StatusRDDDataSet.unpersist()

    /*FLUSH JVM CACHE MEMORY*/
    println("FLUSH JVM CACHE MEMEORY FOR LULD DATA SET ....")
    LuldRDD.unpersist()
    LuldRDDDataSet.unpersist()

    /*FLUSH JVM CACHE MEMORY*/
    println("FLUSH JVM CACHE MEMEORY FOR LULD DATA SET ....")
    QuoteRDD.unpersist()
    QuoteHashMap.empty

    /*Return exit code 0 if all the above steps are successful*/
    System.exit(0)

  }

}
