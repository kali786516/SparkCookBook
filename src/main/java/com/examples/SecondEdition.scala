import com.examples.Philosophical
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

//package com.examples
object SecondEdition {

  def main(args: Array[String]): Unit =
  {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("ScalaWordCount").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val a=List(1,2,3)
    val b=List(3,4,5)
    val c=a::b
    print(c)
    val d=a:::b
    print(d)

    val product1=List("brush","toothpaste","colgate")

    val product2=List("towel","soap")

    val toilet=product1 ::: product2

    print(toilet)

    //absolute value
    print(-10.5 abs)
    //print max
    print(0 max 5)
    //print round
    print(-2.8 round)
    //capitalize
    print("kali" capitalize)

    //tuple example
    val t=("kali","charan")
    print(t._1)
    // map example
    val k=Map("Al" -> "Alabama","AK" -> "Alaska")
    // print(k)

    k foreach {case (key,value) => println(key + "..." + value)}

    //for (word <- k) yield k {case (key,value) => println(key + ".." + value)}



    val comments = sc.parallelize(List(("u1", "one two one"), ("u2", "three four three")))

    //comments.flatMap({case(key,value) => )})

    //for (word <- comments.split(" ")) yield(((user, word), 1))

    //println(for (i <- 1 to 5) yield i)

    case class Trade(TradeID : Int,TradePrice :Int,TraderName :String)
    val words=sc.parallelize(List("123","300","kali"))

    val testwords=words.flatMap(x => x.split(" "))

    for (shit <- testwords) yield (shit)
    //for (tradedetails <-words) yield tradedetails.flatMap(x => x.split)
    //for (trade)


    //case statement

    val food="salt"

    val firend= food match {
      case "salt" => println("pepper")
      case "chips" => println("salsa")
      case _ => println("huh?")
    }

    // returns a row as a seq

    def makeRowSeq(row: Int) =
      for (col <- 1 to 10) yield {
        val prod= (row * col).toString
        val padding=" " * (4-prod.length)
        padding + prod
      }

    def makeRow(row :Int) =makeRowSeq(row).mkString

    def multiTable() ={

      val tableSeq=
        for (row <- 1 to 10)
          yield makeRow(row)

      tableSeq.mkString("\n")
    }

    def processFile(filename: String,width: Int): Unit =
    {
      val source=Source.fromFile(filename)
      for (line <- source.getLines())
        processLine(filename,width,line)
    }

    def processLine(filename: String,width: Int,line:String){
      if (line.length > width)
        println(filename + " " +line.trim)
    }

    def sum(a:Int,b:Int,c:Int)= a+b+c
    //val d=sum __

    //def error example

    def error(message: String) : Nothing= throw new RuntimeException (message)

    def divide(x: Int,y:Int): Int= if (y!=0) x/y else error("cant divide by zero")

    class frog extends Philosophical
    {
      override def toString="green"
    }

    val forog=new frog
     forog.philosophize()



  }

}
