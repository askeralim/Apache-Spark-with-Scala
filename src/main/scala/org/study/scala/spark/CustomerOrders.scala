package org.study.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerOrders extends App {
  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")
    val data = sc.textFile("../../Resources/SparkScala/customer-orders.csv")
    val productData = data.map(x=>x.split(",")).map(fields=> (fields(1).toInt, fields(2).toFloat))
    //productData.foreach(println)
    val totalProductSum = productData.reduceByKey((x,y)=>x+y)
    //Printing the total paid by per customer, The sorting is not working need to improve it by clustered ordering
    data.map(x=>x.split(",")).map(fields=> (fields(0).toInt, fields(2).toFloat)).reduceByKey((x,y)=>x+y).sortByKey().foreach(println)
    //totalProductSum.foreach(println)
  }
}
