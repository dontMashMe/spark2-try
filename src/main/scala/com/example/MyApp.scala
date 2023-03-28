package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import data_process.DataProcessor


object MyApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR) //reduce compile output
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("MyApp")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .config("dfs.client.read.shortcircuit.skip.checksum", "true")
      .getOrCreate()


    val fileSeparator = java.io.File.separator
    val dataSources = Array(f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore_user_reviews.csv",
      f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore.csv")

    val dataProcessor = new DataProcessor(spark, dataSources)
    dataProcessor.show(dataProcessor.df_1)
    dataProcessor.show(dataProcessor.df_2)

    dataProcessor.save(dataProcessor.df_2, "best_apps.csv")

  }
}
