package hdm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object GroupBySum extends App {
  /** Our main function where the action happens */
  override def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../../Resources/hdm/TR143DownloadDiagnosticsReport_file4_2.csv")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.filter(x=>x.toString.indexOf("Pending")>0  )
                    .map(x => x.toString().split(",")(1))
                   // .map(x=>(x(0),))//,x.toString().split("\t")(2)))
    ratings.foreach(println)
    //ratings.foreach(println)
    // Count up how many times each value (rating) occurs
    //val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples, Sort with Lowest Rating First
    //val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
   // sortedResults.foreach(println)
  }
}
