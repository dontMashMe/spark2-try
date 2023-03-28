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
      //since this is a standalone cluster, set the local node as a master.
      .setMaster("local[*]")

    /*
        Pass the configuration object using the config() method and build a session.
    */
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val fileSeparator = java.io.File.separator
    val dataSources = Array(f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore_user_reviews.csv",
      f"data$fileSeparator%sinput$fileSeparator%sgoogleplaystore.csv")

    val dataProcessor = new DataProcessor(spark = spark, data_source = dataSources)
    dataProcessor.show(dataProcessor.df_1)

  }
}
