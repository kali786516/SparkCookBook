package com.examples

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

case class Person(first_name:String,last_name:String,age:Int)

object SparkParquet {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("My App3")
    val sc = new SparkContext(conf)

    val Persontwo = Person

    val personRdd = sc.textFile("C:\\scala_test_folder\\person.txt").map(_.split(" ")).map(p => Person(p(0), p(1), p(2).toInt))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val persondf = sc.parallelize(personRdd.collect())

    //persondf.registerTempTable("person")

    //val sixtyPlus=sql("select * from person where age > 60")

    //sixtyPlus.collect.foreach(println)

    //sixtyplus.saveAsParquetFile("C:\\scala_test_folder\\person_parquet.txt")

  }
}
