package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._

object LinearRegressionDataFrameDataset {
  
  case class RegressionSchema(label: Double, features_raw: Double)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()
      
    // Load up our page speed / amount spent data in the format required by MLLib
    // (which is label, vector of features)
      
    // In machine learning lingo, "label" is just the value you're trying to predict, and
    // "feature" is the data you are given to make a prediction with. So in this example
    // the "labels" are the first column of our data, and "features" are the second column.
    // You can have more than one "feature" which is why a vector is required.
    val regressionSchema = new StructType()
      .add("label", DoubleType, nullable = true)
      .add("features_raw", DoubleType, nullable = true)

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .schema(regressionSchema)
      .csv("data/regression.txt")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("features_raw")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
        .select("label","features")
     
    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)
    
    // Now create our linear regression model
    val lir = new LinearRegression()
      .setRegParam(0.3) // regularization 
      .setElasticNetParam(0.8) // elastic net mixing
      .setMaxIter(100) // max iterations
      .setTol(1E-6) // convergence tolerance
    
    // Train the model using our training data
    val model = lir.fit(trainingDF)
    
    // Now see if we can predict values in our test data.
    // Generate predictions using our linear regression model for all features in our 
    // test dataframe:
    val fullPredictions = model.transform(testDF).cache()
    
    // This basically adds a "prediction" column to our testDF dataframe.
    
    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "label").collect()
    
    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    // Stop the session
    spark.stop()

  }
}