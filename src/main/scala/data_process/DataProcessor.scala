package data_process

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import file_handler.CSVHandler
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._

class DataProcessor(spark: SparkSession, data_source: Array[String]) {
  private val _dFUserReviews = preClearData(CSVHandler.load(spark, data_source(0)))
  private val _dFPlayStore = preClearData(CSVHandler.load(spark, data_source(1)))

  val df_1: DataFrame = getAvgSentimentPolarityOfApps
  val df_2: DataFrame = getHighRatingApps
  val df_3: DataFrame = getPlayStoreTrans

  def show(df: DataFrame): Unit = {
    df.show()
  }

  def save(df: DataFrame, fileName: String, delimiter: String = ","): Unit = {
    CSVHandler.saveToCsv(df, fileName, delimiter)
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
    val df_1Temp = _dFUserReviews.groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    df_1Temp.na.fill(0, Seq("Average_Sentiment_Polarity"))
  }

  private def getHighRatingApps: DataFrame = {
    val df_2Temp = _dFPlayStore
      .filter(col("Rating").isNotNull &&
        !isnan(col("Rating")) &&
        col("Rating").cast("double") >= 4.0)
      .sort(desc("Rating"))
    df_2Temp
  }

  private def getPlayStoreTrans: DataFrame = {
    // perform initial grouping, casting and renaming.
    val filteredDf = _dFPlayStore
      .withColumn("Genres", explode(split(col("Genres"), ";")))
      .withColumn("Genres", trim(col("Genres")))
      .groupBy("App")
      .agg(
        collect_set("Category").alias("Categories"),
        max("Rating").cast("Double").as("Rating"),
        max("Reviews").cast("Long").as("Reviews"),
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
          .otherwise(regexp_replace(col("Price"), "\\$", "").cast("Double") / 1.19)
      )

    // find the max value
    val maxReviewsDf = filteredDf.groupBy("App").agg(max("Reviews").as("maxReviews"))
    val finalDf = filteredDf.join(maxReviewsDf, Seq("App"))
      .filter(col("Reviews") === col("maxReviews"))
      .drop("maxReviews")
      .filter(size(col("Categories")) >= 2)

    finalDf.show()
    finalDf.printSchema()
    finalDf

  }

}
