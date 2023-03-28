package data_process

import org.apache.spark.sql.{DataFrame, SparkSession}
import file_handler.CSVHandler
import org.apache.spark.sql.functions._

class DataProcessor(spark: SparkSession, data_source: Array[String]) {
  private val _dFUserReviews = CSVHandler.load(spark, data_source(0))
  private val _dFPlayStore = CSVHandler.load(spark, data_source(1))

  val df_1: DataFrame = getAvgSentimentPolarityOfApps
  def show(df: DataFrame): Unit = {
    df.show()
  }

  def save(df: DataFrame, fileName: String, delimiter: String = ","): Unit = {
    CSVHandler.saveToCsv(df, fileName, delimiter)
  }

  private def getAvgSentimentPolarityOfApps: DataFrame = {
    val df_1Temp = _dFUserReviews.groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    df_1Temp.na.fill(0, Seq("Average_Sentiment_Polarity"))
  }


}
