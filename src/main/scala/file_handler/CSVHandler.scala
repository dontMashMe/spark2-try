package file_handler
import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVHandler {
  private val fileSeparator: String = java.io.File.separator

  def load(spark: SparkSession, path: String): DataFrame = {
    println(f"Loading $path to a DataFrame.")

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def saveToCsv(df: DataFrame, fileName: String, delimiter: String = ","): Unit = {
    val outputPath: String = f"data$fileSeparator%soutput$fileSeparator%s" + fileName
    println(f"Writing $df to a file = $outputPath")
    df.write
      .option("header", "true")
      .option("delimiter", delimiter)
      .mode("overwrite")
      .csv(outputPath)
  }
}
