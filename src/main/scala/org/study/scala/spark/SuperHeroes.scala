package org.study.scala.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark.SparkContext._
import org.apache.spark._

import scala.io.{Codec, Source}

object SuperHeroes extends App {
  def countCoOccurances(line:String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }

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

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile("../../Resources/SparkScala/marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    // Load up the superhero co-apperarance data
    val lines = sc.textFile("../../Resources/SparkScala//marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurances)
    val totalFriendsByCharacter = pairings.reduceByKey((x,y)=>x+y)
    val flipped = totalFriendsByCharacter.map(x=>(x._2, x._1))
   // pairings.reduce((x,y)=>x+y)
   // pairings.foreach(println)
    println("=====================")
    //totalFriendsByCharacter.foreach(println)
    println("=====================")
    val mostPopular = flipped.max()
    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
  }
}
