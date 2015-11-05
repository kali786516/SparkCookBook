package com.examples

/**
 * Created by kalit_000 on 13/09/2015.
 */

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
//import org.apache.spark._
//import org.apache.spark.SparkContext._
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

class HadoopKeyValue extends SparkTrait
{

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  //val conf = new SparkConf().setMaster("local").setAppName("My App3")
  //val sc=new SparkContext()

  val currencyFile=sc.newAPIHadoopFile("C:\\scala_test_folder\\na.txt",classOf[KeyValueTextInputFormat],classOf[Text],classOf[Text])

  // convert to tuple of (text,text) to tuple of (string,string)

  val currencyRdd=currencyFile.map(x=> (x._1.toString,x._2.toString))

  currencyRdd.count()

  currencyRdd.collect().foreach(println)


}
