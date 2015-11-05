package com.examples

/**
 * Created by kalit_000 on 12/09/2015.
 */

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.spark.SparkContext._


object WordCountFromFile extends SparkTrait{

  def main(args: Array[String])
  {

    //Logger.getLogger("org").setLevel(Level.WARN)
    //Logger.getLogger("akka").setLevel(Level.WARN)

    //val conf = new SparkConf().setMaster("local").setAppName("My App2")
    //val sc=new SparkContext(conf)

    val input = args(0)

    val filename=sc.textFile("C:\\scala_test_folder\\test_wordcount.txt")

    //val filename=sc.textFile(input)

    val fileflat=filename.flatMap(x=>x.split(" "))

    val filemap=fileflat.map(x=>(x,1))

    val countofwords=filemap.reduceByKey(_+_)

    countofwords.collect().foreach(println)

    filename.unpersist()
    fileflat.unpersist()
    filemap.unpersist()
    countofwords.unpersist()

    print("hi kali charan")

    print("lets run the other class HadoopKeyValuueMy")

    class HadoopKeyValueMy
    {

      val currencyFile=sc.newAPIHadoopFile("C:\\scala_test_folder\\na.txt",classOf[KeyValueTextInputFormat],classOf[Text],classOf[Text])

      // convert to tuple of (text,text) to tuple of (string,string)

      val currencyRdd=currencyFile.map(x=> (x._1.toString,x._2.toString))

      currencyRdd.count()

      currencyRdd.collect().foreach(println)

    }

    val k=new HadoopKeyValueMy()

    k

  }

}
