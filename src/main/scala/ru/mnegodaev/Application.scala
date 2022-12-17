package ru.mnegodaev

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode.Complete
import ru.mnegodaev.transformer.Transformer
import ru.mnegodaev.util.{JobBase, TryWithResources}

object Application extends JobBase with TryWithResources {
  def main(args: Array[String]): Unit = {
    autoClose(SparkSession.builder().getOrCreate()) {
      sparkSession =>
        run(sparkSession)
    }
  }

  def run(spark: SparkSession): Unit = {
    val sourcePath = spark.conf.get("source_path")
    val targetTable = spark.conf.get("product_table_name")
    val actualSchema = inferSchema(spark, s"$sourcePath/part_1.csv")

    val df = spark.readStream
      .schema(actualSchema)
      .option("sep", ",")
      .option("header", "true")
      .option("enforceSchema", "false")
      .option("mode", "FAILFAST")
      .csv(sourcePath)

    Transformer.transform(df)
      .writeStream
      .format("delta")
      .outputMode(Complete())
      .queryName(targetTable)
      .option("checkpointLocation", s"checkpoint-$targetTable")
      .start(f"default/$targetTable")
  }
}
