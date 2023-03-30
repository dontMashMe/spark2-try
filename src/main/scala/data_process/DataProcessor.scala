package data_process

import org.apache.spark.sql.{DataFrame, SparkSession}
import file_handler.FileHandler
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._

class DataProcessor(spark: SparkSession, data_source: Array[String]) {
  private val _dFUserReviews = preClearData(FileHandler.load(spark, data_source(0)))
  private val _dFPlayStore = preClearData(FileHandler.load(spark, data_source(1)))

  val df_1: DataFrame = getAvgSentimentPolarityOfApps
  val df_2: DataFrame = getHighRatingApps
  val df_3: DataFrame = getCleanedPlayStore
  val df_Joined: DataFrame = getJoinedPlayStoreWithPolarity

  val df_4: DataFrame = getGooglePlaysStoreMetrics

  def show(df: DataFrame): Unit = {
    df.show()
  }

  def saveAsCsv(df: DataFrame, fileName: String, delimiter: String = ","): Unit = {
    FileHandler.saveToCsv(df, fileName, delimiter)
  }

  def saveAsParquet(df: DataFrame, fileName: String): Unit = {
    FileHandler.saveAsParquet(df, fileName)
  }

  /**
   * Remove all garbage non-ascii characters from imported data.
   * */
  private def preClearData(df: DataFrame): DataFrame = {
    val dfCleaned = df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumn(colName, regexp_replace(df.col(colName),
        "[^ -~]", ""))
    }
    dfCleaned
  }

  private def getAvgSentimentPolarityOfApps: DataFrame = {
    val df_1Temp = _dFUserReviews
      .na.drop(Seq("Sentiment_Polarity"))
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    df_1Temp
  }

  private def getHighRatingApps: DataFrame = {
    val df_2Temp = _dFPlayStore
      .filter(col("Rating").isNotNull &&
        !isnan(col("Rating")) &&
        col("Rating").cast("double") >= 4.0)
      .sort(desc("Rating"))
    df_2Temp
  }

  private def getCleanedPlayStore: DataFrame = {
    // perform initial grouping, casting and renaming.
    val filteredDf = _dFPlayStore
      //handle the ; separated genre values
      .withColumn("Genres", explode(split(col("Genres"), ";")))
      .withColumn("Genres", trim(col("Genres")))
      .groupBy("App")
      .agg(
        collect_set("Category").alias("Categories"),
        max("Rating").cast("Double").as("Rating"),
        max("Reviews").cast("Long").as("Reviews"),
        // remove non-numerics
        max(regexp_extract(col("Size"), "(\\d+\\.?\\d*)", 1)).cast("Double").as("Size"),
        max("Installs").as("Installs"),
        max("Type").as("Type"),
        max("Price").as("Price"),
        max("Content Rating").as("Content_Rating"),
        collect_set(col("Genres")).alias("Genres"),
        to_date(max("Last Updated"), "MMM dd, yyyy").as("Last_Updated"),
        max("Current Ver").as("Current_version"),
        max("Android Ver").as("Minimum_android_version")
      )
      .withColumnRenamed("Content_Rating", "Content_Rating")
      .withColumn("Price",
        when(col("Price") === "0", 0.0)
          //remove the $ sign and convert to euro by dividing with 1.19 (also can multiply by 0.81)
          .otherwise(regexp_replace(col("Price"), "\\$", "").cast("Double") / 1.19)
      )

    // find the max value
    val maxReviewsDf = filteredDf.groupBy("App").agg(max("Reviews").as("maxReviews"))
    val finalDf = filteredDf.join(maxReviewsDf, Seq("App"))
      .filter(col("Reviews") === col("maxReviews"))
      .drop("maxReviews")

    finalDf
  }

  private def getJoinedPlayStoreWithPolarity: DataFrame = {
    df_3.join(df_1, Seq("App"))
  }

  private def getGooglePlaysStoreMetrics: DataFrame = {

    // Join cleanedPlayStoreDf with _dFUserReviews to get Sentiment_Polarity for each app
    val joinedDf = df_3.join(
      df_1.select("App", "Average_Sentiment_Polarity"),
      Seq("App"),
      "left"
    )

    // Explode Genres and group by Genre to get the required metrics
    val genreDf = joinedDf.select(explode(col("Genres")).as("Genre"),
                                  col("Rating"),
                                  col("Average_Sentiment_Polarity"))
      .na.drop(Seq("Rating"))
      .groupBy("Genre")
      .agg( count("Rating").as("Count"),
            avg("Rating").as("Average_Rating"),
            avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity"))

    genreDf

  }

}
