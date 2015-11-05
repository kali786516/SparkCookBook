package com.examples

/**
 * Created by kalit_000 on 17/09/2015.
 */
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._


object HiveWordCount {

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("HiveWordCount").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)

    val hc=new HiveContext(sc)

    hc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.KP_TEST  (user_name string ,COMMENTS STRING )ROW FORMAT DELIMITED FIELDS TERMINATED BY '001'  STORED AS TEXTFILE LOCATION '/data/kp/kp_test' ")

    val op=hc.sql("select user_name,COLLECT_SET(text) from (select user_name,concat(sub,' ',count(comments)) as text  from default.kp_test LATERAL VIEW explode(split(comments,',')) subView AS sub group by user_name,sub)w group by user_name")

    op.collect.foreach(println)


  }

}
