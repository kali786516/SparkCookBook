package com.examples

/**
 * Created by kalit_000 on 20/09/2015.
 */

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark._
import  org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingKafkaTwo {

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreaming").set("spark.executor.memory", "1g")
    val sc=new SparkContext(conf)
    val ssc= new StreamingContext(sc,Seconds(2))

    val zkQuorm="localhost:2181"
    val group="test-group"
    val topics="first"
    val numThreads=1

    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap

    val lineMap=KafkaUtils.createStream(ssc,zkQuorm,group,topicMap)

    val lines=lineMap.map(_._2)

    lines.print

    ssc.checkpoint("C:\\scalatutorials\\sparkstreaming_checkpoint_folder")

    ssc.start()

    ssc.awaitTermination()
  }

}
