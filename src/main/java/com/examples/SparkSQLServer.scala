package com.examples

/**
 * Created by kalit_000 on 25/09/2015.
 */


import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import java.sql.{ResultSet, DriverManager, Connection}


object SparkSQLServer {

    def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkProducerMSSQL")
    val sc = new SparkContext(conf)

    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val url = "jdbc:sqlserver://localhost;user=admin;password=oracle;database=AdventureWorks2014"
    val username = "admin"
    val password = "oracle"

    var connection: Connection = null

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select top 10 CustomerID,AccountNumber from AdventureWorks2014.dbo.Customer")
    resultSet.setFetchSize(10);
    val columnnumber = resultSet.getMetaData().getColumnCount.toInt


      /*OP COLUMN NAMES*/
      var i = 0.toInt;
      for (i <- 1 to columnnumber.toInt)
      {
        val columnname=resultSet.getMetaData().getColumnName(i)
         println("Column Names are:- %s".format(columnname))
      }

      /*OP DATA*/
      while (resultSet.next())
     {
       var list = new java.util.ArrayList[String]()
          for (i <- 1 to columnnumber.toInt)
        {
          list.add(resultSet.getObject(i).toString())
          //println("Column Names are:- %s".format(columnname))
        }
       println(list)
     }
    connection.close()
  }
}


