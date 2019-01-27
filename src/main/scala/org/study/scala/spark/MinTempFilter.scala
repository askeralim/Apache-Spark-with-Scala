package org.study.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.min

object MinTempFilter extends App {
  override def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CountFriends")
    val lines = sc.textFile("../../Resources/SparkScala/1800.csv")
    lines.map(parseLine).foreach(println)
    val minTemp = lines.map(parseLine).filter(x=> x._2 =="TMAX")
    println("Printing Distinct: ")
   // lines.map(parseLine).map(x=>x._1).distinct().foreach(println)
    minTemp.foreach(println)
    println("Printing Distinct: ")
    minTemp.map(x=>x._1).distinct().foreach(println)
    val stationTemp = minTemp.map(x=> (x._1, x._3.toFloat))
    val minTempByStation = stationTemp.reduceByKey((x,y) => min(x,y))
    //COnverting Spark Object to Scala Object
    val results = minTempByStation.collect()
    println("Printing Minimum")
  for(result <- results){
    val temp = result._2
    val formatTemp = f"$temp%.2f F"
    println(s"$result._1 minimum Temperature :$formatTemp")

  }
    println("Printing Minimum")
    minTempByStation.foreach(println)
  }
  def parseLine(line:String)={
    val fields = line.split(",")
    val stationId=fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.01f*(9.0f/5.0f)+32.0f
    (stationId,entryType,temperature)
  }
}
