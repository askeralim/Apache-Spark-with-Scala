package org.study.scala.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

object MovieRatingsCounterBroadCast extends App {
  def loadMovieNames : Map[Int,String] ={
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("../../Resources/ml-100k/u.item").getLines()
    for(line<- lines){
      var fields = line.split('|')
      if(fields.length>1)
        movieNames += (fields(0).toInt -> fields(1))
    }
    return movieNames
  }
  /** Our main function where the action happens */
  override def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    //Broadcasting the names to all clusters here.
    var nameDict = sc.broadcast(loadMovieNames)

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../../Resources/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(1))//,x.toString().split("\t")(2)))
    //ratings.foreach(println)
    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    //Flip Key and Value
    val flipped = results.map(s=> (s._2, s._1))
    // Sort the resulting map of (rating, count) tuples, Sort with Lowest Rating First
    val sortedResults = flipped.toSeq.sortBy(_._1)
    // Print each result on its own line.
    sortedResults.foreach(println)

    //Sorted Movies with names
    val sortedResultsWithNames = sortedResults.map(s=>(nameDict.value(s._2.toInt),s._1))

    // Print each result with Names on its own line.
    sortedResultsWithNames.foreach(println)
  }
}
