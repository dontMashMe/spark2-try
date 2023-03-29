package file_handler

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.io.File
import java.nio.file.{Files, StandardCopyOption}

object FileHandler {
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
   * Workaround since I can't figure out how to correctly name the created files
   *
   * Loop through the output path and remove the .crc & SUCCESS  files, then rename the created file to
   * the given name and move it to data/output.
   *
   * On exit, remove the directory created by the save methods.
   * */
  private def handleCreatedOutput(filename: String): Unit = {
    val directory = new File(outputPath+f"dir_$filename")
    if (directory.isDirectory) {
      val files = directory.listFiles()
      for (file <- files) {
        //remove .crc and SUCCESS files
        if (file.getName.contains("crc") || (!file.getName.contains(".csv") && !file.getName.contains("parquet"))) {
          //println("Deleting = " + file.getName)
          file.delete()
        } else {
          val dest = new File(outputPath + filename)

          //println(f"Moving ${file.toPath} -> ${dest.toPath}")
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
    println(f"Writing $df to a CSV file = $outputPath" + fileName)
    df
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("escape", "")
      .mode("overwrite")
      .csv(outputPath+f"dir_$fileName")

    handleCreatedOutput(fileName)
  }

  def saveAsParquet(df: DataFrame, fileName: String): Unit = {
    println(f"Writing $df to a parquet file = $outputPath" + fileName)

    df
      .coalesce(1)
      .write
      .format("parquet")
      .option("compression", "gzip")
      .save(outputPath+f"dir_$fileName")

    handleCreatedOutput(fileName)

  }
}
