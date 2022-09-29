package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {
  
  // Function to extract the hero ID and number of connections from each line
  def countCoOccurrences(line: String): (Int, Int) = {
    val elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      Some(fields(0).trim().toInt, fields(1))
    } else None // flatmap will just discard None results, and extract data from Some results.
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")   
    
    // Build up a hero ID -> name RDD
    val names = sc.textFile("data/marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    
    // Load up the superhero co-appearance data
    val lines = sc.textFile("data/marvel-graph.txt")
    
    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurrences)
    
    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
    
    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )
    
    // Find the max # of connections
    val mostPopular = flipped.max()
    
    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2).head
    
    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.") 
  }
  
}
