package file_handler

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

object CSVHandler {
  private val fileSeparator: String = java.io.File.separator
  private val outputPath: String = f"data$fileSeparator%soutput$fileSeparator"

  /**
   * Load the given CSV file to a DataFrame.
   * */
  def load(spark: SparkSession, path: String): DataFrame = {
    println(f"Loading $path to a DataFrame.")

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  /**
   * Workaround since I can't figure out how to correctly name the created CSV files.
   *
   * Loop through the output path and remove the .crc files, then rename the CSV file to
   * the given name and move it to data/output.
   *
   * On exit, remove the directory created by the saveToCsv() method.
   * */
  private def handleCreatedOutput(filename: String): Unit = {
    val directory = new File(outputPath)
    if (directory.isDirectory) {
      val files = directory.listFiles()
      for (file <- files){
        if (file.getName.contains(".crc")) {
          file.delete()
        } else {
          val dest = new File(outputPath + fileSeparator + filename)
          Files.move(file.toPath, dest.toPath, StandardCopyOption.ATOMIC_MOVE)
        }

      }
    }
    directory.deleteOnExit()
  }
  /**
   * Writes the given DataFrame to a CSV file.
   *
   * Additional handling needed to remove .crs files and default Spark behavior when
   * writing a DataFrame to a CSV file (done by handleCreatedOutput)
   *
   * */

  def saveToCsv(df: DataFrame, fileName: String, delimiter: String = ","): Unit = {
    println(f"Writing $df to a file = $outputPath")

    df
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("escape", "\"") // schema breaks if the delimiter is in the data rows without escaping
      .mode("overwrite")
      .csv(outputPath)

    handleCreatedOutput(fileName)
  }
}
