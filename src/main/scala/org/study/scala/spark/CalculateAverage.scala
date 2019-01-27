package org.study.scala.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext._

object CalculateAverage extends App{

  /** Our main function where the action happens */
  override def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "CountFriends")
    /**
      * Calculate the Average score based on Subject. from provided sequences.
      */
    val inputRdd = sc.parallelize(Seq(("maths", 50), ("maths", 60), ("english", 65)))

    val mapped = inputRdd.mapValues(mark => (mark, 1))
    val reducedSum = mapped.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    reducedSum.foreach(println)
    reducedSum.mapValues(x=> x._1/x._2).foreach(println)
    /**
      * Calculate the Average number of Friends based on Age. By Reading from the File.
      */
    //Reading from a File
    val lines = sc.textFile("../../Resources/SparkScala/fakefriends.csv")
    val ageRDD = lines.flatMap(x=>x.split(",") match{
                    case Array(a,b,c,d) => Some(c.toInt,d.toInt)
                    case _ => None
                  })
    //According to the course it should work with lines.map(parseLine), But not working here.
    //lines.map(parseLine).mapValues()
    ageRDD.foreach(println)
    val ageMapped = ageRDD.mapValues(count => (count, 1))
    val reducedAge = ageMapped.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    println("Printing Reduced Age :")
    reducedAge.foreach(println)
    println("Printing Average Friends Count by Age:")
    reducedAge.mapValues(x=> x._1/x._2).foreach(println)
    println("Printing Average Friends Count by Age [Sorted]:")
    val avg = reducedAge.mapValues(x=> x._1/x._2).sortByKey(true).foreach(println)
   // avg.sorsortBy((x,y) =>x<y).foreach(println)
  }

  def parseLine(line:String){
    val fields = line.split(",")
    val age:Int=fields(1).toInt
    val count:Int = fields(2).toInt
    return (age,count)
  }
}
