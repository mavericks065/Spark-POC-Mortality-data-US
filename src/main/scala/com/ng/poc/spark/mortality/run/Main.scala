package com.ng.poc.spark.mortality.run

import com.ng.poc.spark.mortality.report.statistics.{BaseStatistics, StatisticsCoreConfig}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql._

object Main {

  val appName = "Mortality in the US report"
  val logger = LogManager.getLogger(Main.getClass)

  def main(args: Array[String]) {

    val sparkBuilder = SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)

    val spark = sparkBuilder.getOrCreate()

    val statisticsConfig = new StatisticsCoreConfig(spark)
    val baseStatistics = new BaseStatistics(spark)

    logger.info(statisticsConfig)
    logger.info(baseStatistics)

    spark.stop()
  }
}
