package com

import com.examples.SparkTrait
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kalit_000 on 13/09/2015.
 */

case class Person (first_name:String,last_name:String,age:Int)

package object CaseClassTest {

  def main(args: Array[String])
  {


    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("My App3")
    val sc=new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.createSchemaRDD
    //import org.apache.spark.sql._

    val p=sc.textFile("C:\\scala_test_folder\\person.txt")
    val pmap=p.map( x=> x.split(" "))
    //val personrdd=pmap.map( x=> Person(p(0)))

    // incomplete code ....


  }

}
