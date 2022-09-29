package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import scala.io.Codec

/** Find the movies with the most ratings. */
object PopularMoviesNicer {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("data/ml-100k/u.item")
     for (line <- lines.getLines()) {
       val fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
     lines.close()
    
     movieNames
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")  
    
    // Create a broadcast variable of our ID -> movie name map
    val nameDict = sc.broadcast(loadMovieNames())
    
    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")
    
    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    
    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    
    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    
    // Sort
    val sortedMovies = flipped.sortByKey()
    
    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
    
    // Collect and print results
    val results = sortedMoviesWithNames.collect()
    
    results.foreach(println)
  }
  
}

