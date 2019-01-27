package org.study.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object ReadFile extends App{
  override def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CountFriends")
    val lines = sc.textFile("../../Resources/SparkScala/fakefriends.csv")
    //lines.map(parseLine).mapValues
  }
  def parseLine(line:String){
    val fields = line.split(",")
    val age:Int=fields(1).toInt
    val count:Int = fields(2).toInt
    return (age,count)
  }
}
