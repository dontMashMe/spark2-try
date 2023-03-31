package com.example
import org.apache.log4j.{Level, Logger}
import data_process.DataProcessor

object MyApp {
  def main(args: Array[String]): Unit = {

    // reduce compile output
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val fileSeparator = java.io.File.separator
    //  data/input/_.csv
    val dataSources = Array(f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore_user_reviews.csv",
                            f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore.csv")

    val spark = SparkSessionBuilder.getSparkSession

    val dataProcessor = new DataProcessor(spark, dataSources)

    // part 1
    //dataProcessor.show(dataProcessor.df_1)

    // part 2
    //dataProcessor.show(dataProcessor.df_2)
    dataProcessor.saveAsCsv(dataProcessor.df_2, "best_apps.csv")

    // part 3
    //dataProcessor.show(dataProcessor.df_3)

    // part 4
    //dataProcessor.show(dataProcessor.df_Joined)
    dataProcessor.saveAsParquet(dataProcessor.df_Joined, "googleplaystore_cleaned")

    // part 5
    //dataProcessor.show(dataProcessor.df_4)
    dataProcessor.saveAsParquet(dataProcessor.df_4, "googleplaystore_metrics")

  }
}
