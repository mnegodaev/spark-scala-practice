package ru.mnegodaev.base

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class SparkSpec extends AnyFunSpec with BeforeAndAfter {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config("source_path", "src/test/resources/MiningProcess_Flotation_Plant_Database")
    .config("product_table_name", "test_data_product")
    .getOrCreate()

  def waitForFilesIn(path: String): Unit = {
    var counter = 0
    while (counter < 100 && FileUtils.sizeOfDirectory(new File(path)) < 1) {
      Thread.sleep(1000)
      counter += 1
    }
  }
}
