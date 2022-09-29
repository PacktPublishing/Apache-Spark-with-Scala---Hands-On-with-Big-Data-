package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.functions._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomerDataset {

  case class CustomerOrders(cust_id: Int, item_id: Int, amount_spent: Double)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val customerOrdersSchema = new StructType()
      .add("cust_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)

    import spark.implicits._
    val customerDS = spark.read
      .schema(customerOrdersSchema)
      .csv("data/customer-orders.csv")
      .as[CustomerOrders]
    
    val totalByCustomer = customerDS
      .groupBy("cust_id")
      .agg(round(sum("amount_spent"), 2)
        .alias("total_spent"))

    totalByCustomer.show(totalByCustomer.count.toInt)
  }
}

