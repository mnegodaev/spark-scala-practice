package ru.mnegodaev.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, round}
import org.apache.spark.sql.types.TimestampType

object Transformer {
  private val groupByColumns = List("pcnt_iron_feed", "date")

  def transform(df: DataFrame): DataFrame = {
    val transformedDf = df.columns
      .foldLeft(df)((dataframe: DataFrame, columnName: String) => {
        dataframe.withColumnRenamed(
          columnName,
          columnName.toLowerCase.trim.replace(" ", "_").replace("%", "pcnt")
        )
      }).withColumn("date", col("date").cast(TimestampType))

    val aggColumns = transformedDf.columns
      .filterNot(groupByColumns.contains)
      .map(column => {
        round(avg(column), 2).as(s"${column}_avg")
      })

    val aggregatedDf = transformedDf.withWatermark("date", "1 seconds")
      .groupBy(groupByColumns.map(col): _*)
      .agg(aggColumns.head, aggColumns.tail: _*)

    aggregatedDf
  }
}
