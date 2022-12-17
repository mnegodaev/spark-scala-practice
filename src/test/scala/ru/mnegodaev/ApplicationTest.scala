package ru.mnegodaev

import org.apache.commons.io.FileUtils
import ru.mnegodaev.base.SparkSpec

import java.io.File

private class ApplicationTest extends SparkSpec {
  val targetPath = f"default/${spark.conf.get("product_table_name")}"
  val checkpointPath = f"checkpoint-${spark.conf.get("product_table_name")}"

  before {
    FileUtils.forceMkdir(new File(targetPath))
    FileUtils.cleanDirectory(new File(targetPath))
    FileUtils.deleteQuietly(new File(checkpointPath))
  }

  describe("run") {
    it("should stream aggregated values to the output path") {
      Application.run(spark)
      waitForFilesIn(targetPath)
      val actual = spark.read.parquet(targetPath)

      assert(actual.columns === Array(
        "pcnt_iron_feed",
        "date",
        "pcnt_silica_feed_avg",
        "starch_flow_avg",
        "amina_flow_avg",
        "ore_pulp_flow_avg",
        "ore_pulp_ph_avg",
        "ore_pulp_density_avg",
        "flotation_column_01_air_flow_avg",
        "flotation_column_02_air_flow_avg",
        "flotation_column_03_air_flow_avg",
        "flotation_column_04_air_flow_avg",
        "flotation_column_05_air_flow_avg",
        "flotation_column_06_air_flow_avg",
        "flotation_column_07_air_flow_avg",
        "flotation_column_01_level_avg",
        "flotation_column_02_level_avg",
        "flotation_column_03_level_avg",
        "flotation_column_04_level_avg",
        "flotation_column_05_level_avg",
        "flotation_column_06_level_avg",
        "flotation_column_07_level_avg",
        "pcnt_iron_concentrate_avg",
        "pcnt_silica_concentrate_avg"
      ))
    }
  }
}
