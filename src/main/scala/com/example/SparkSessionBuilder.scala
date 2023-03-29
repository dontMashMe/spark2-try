package com.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
object SparkSessionBuilder {

  private val conf = new SparkConf()
    .setAppName("MyApp")
    .setMaster("local[*]")

  private val spark = SparkSession
    .builder()
    .config(conf)
    .config("dfs.client.read.shortcircuit.skip.checksum", "true")
    .config("spark.sql.parquet.compression.codec", "gzip")
    .getOrCreate()

  def getSparkSession: SparkSession = {
    spark
  }

}
