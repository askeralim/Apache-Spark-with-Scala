package org.study.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCount extends App {
  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")
    val data = sc.textFile("../../Resources/SparkScala/book.txt")
    //rddSolution(data)
    wordCountSparkWay(data)
  }
  def rddSolution(data:RDD[String]): Unit ={
    //val words = data.flatMap(x=>x.split(" "))
    //With Regular Expression
    val words = data.flatMap(x=>x.split("\\W+"))
    println(words.foreach(println))
    val lowerCaseWords = words.map(x=>x.toLowerCase())
    println("Printing Words: ")
    //lowerCaseWords.countByValue().toSeq.sortBy(_._2).foreach(println)
  }
  def wordCountSparkWay(data:RDD[String]): Unit ={
    val words = data.flatMap(x=>x.split("\\W+"))
    //println(words.foreach(println))
    val lowerCaseWords = words.map(x=>x.toLowerCase())
    val wordCount = lowerCaseWords.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
    val wordCountSorted = wordCount.map(x=>(x._2,x._1)).sortByKey()
   // wordCountSorted.foreach(println)
    for(wd<-wordCountSorted){
      println(wd._1+" "+wd._2)
    }
    //I tried the following as well But, this is also not sorting correctly. RDD Sorts only the custers.
   // words.map(x=>(x.toLowerCase,1)).groupBy(_._1).mapValues(_.size).map(x=>(x._2,x._1)).sortByKey().foreach(println)
    println("Printing Words: ")

  }
}
