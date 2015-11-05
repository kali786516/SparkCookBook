package com.examples

/**
 * Created by kalit_000 on 19/09/2015.
 */

import groovy.swing.factory.WidgetFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._


class ButtonWithCallbacks (val label: String,val callbacks: List[() => Unit] = Nil) extends Widget {
  def click(): Unit ={
    updateUI()
    callbacks.foreach(f => f())
  }

  protected def updateUI(): Unit={}
}

object ButtonWithCallbacks {

  def apply(label: String,callback: () => Unit) =
      new ButtonWithCallbacks(label,List(callback))

  def apply(label: String) =
      new ButtonWithCallbacks(label,Nil)


  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setMaster("local").setAppName("callbackbutton")
  val sc = new SparkContext(conf)

}
