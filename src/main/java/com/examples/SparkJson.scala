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
object SparkJson {

  def main(args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("My App4")
    val sc = new SparkContext(conf)

    val sqlContext=new SQLContext(sc)

    val person=sqlContext.jsonFile("C:\\scala_test_folder\\person.json")

    person.registerTempTable("person")

    //val sixtyPlus=sql("select * from person where age > 60")

    //sixtyPlus.collect.foreach(println)

    //sixtyPlus.toJSON.saveAsTestFile("C:\\scala_test_folder\\sixtyplus.")



  }

}
