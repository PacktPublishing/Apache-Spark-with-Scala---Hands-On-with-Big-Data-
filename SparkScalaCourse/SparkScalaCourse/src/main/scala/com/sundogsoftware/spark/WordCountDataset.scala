package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCountDataset {

  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    // Read each line of my book into an Dataset
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    // Split into words separated by a space character
    val words = input
      .select(explode(split($"value", " ")).alias("word"))
      .filter($"word" =!= "")

    // Count up the occurrences of each word
    val wordCounts = words.groupBy("word").count()

    // Show the results.
    wordCounts.show(wordCounts.count.toInt)
  }
}

