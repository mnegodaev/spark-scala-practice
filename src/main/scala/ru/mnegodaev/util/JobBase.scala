package ru.mnegodaev.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.util.Properties

class JobBase {
  lazy val config = new Properties()

  def inferSchema(spark: SparkSession, filePath: String): StructType = {
    spark.read.option("sep", ",").option("header", "true").option("inferSchema", "true").csv(filePath).schema
  }
}
