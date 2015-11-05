package com.examples

/**
 * Created by kalit_000 on 20/09/2015.
 */
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
//import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark._
import  org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector._
import org.apache.spark.streaming.kafka._


object SparkKafkaCassandra {

  case class Test(TRADE_ID:Int,TRADE_PRICE: String)

  def main(args:Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").set("spark.cassandra.connection.host", "127.0.0.1").setAppName("testkali")
    val sc=new SparkContext(conf)
    val ssc= new StreamingContext(sc,Seconds(2))

    val zkQuorm="localhost:2181"
    val group="test-group2"
    val topics="trade"
    val numThreads=1
    val broker="localhost:9091"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)

    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap

    val stream=KafkaUtils.createStream(ssc,zkQuorm,group,topicMap)

    stream.map(_._2.split(",")).map(p => Test (p(0).toInt,p(1))).saveToCassandra("sparkstreamingtest","trade_details",SomeColumns("trade_id","trade_price"))

    val lines=stream.map(_._2)

    ssc.checkpoint("C:\\scalatutorials\\kafkacassandrasparkstreaming_checkpoint_folder")

    lines.print

    ssc.start()

    ssc.awaitTermination()
  }
}
