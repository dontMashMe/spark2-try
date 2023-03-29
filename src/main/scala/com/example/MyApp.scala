package com.example
import org.apache.log4j.{Level, Logger}
import com.example.SparkSessionBuilder
import data_process.DataProcessor


object MyApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR) //reduce compile output
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val fileSeparator = java.io.File.separator
    val dataSources = Array(f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore_user_reviews.csv",
      f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore.csv")

    val spark = SparkSessionBuilder.getSparkSession

    val dataProcessor = new DataProcessor(spark, dataSources)
    // part 1
    dataProcessor.show(dataProcessor.df_1)
    //part 2
    dataProcessor.save(dataProcessor.df_2, "best_apps.csv")

  }
}
