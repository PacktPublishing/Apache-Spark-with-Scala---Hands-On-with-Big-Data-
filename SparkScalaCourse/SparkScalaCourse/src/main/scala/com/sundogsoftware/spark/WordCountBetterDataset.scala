package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetterDataset {

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

    // Split using a regular expression that extracts words
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))

    // Normalize everything to lowercase
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords.groupBy("word").count()

    // Show the results.
    wordCounts.show(wordCounts.count.toInt)
  }
}

