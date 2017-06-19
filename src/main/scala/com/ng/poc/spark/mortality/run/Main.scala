package com.ng.poc.spark.mortality.run

import com.ng.poc.spark.mortality.report.statistics.{NationStatistics, NationStatistics$, StatisticsCoreConfig}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql._

object Main {

  val appName = "Mortality in the US report"
  val logger = LogManager.getLogger(Main.getClass)

  def main(args: Array[String]) {

    val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US.csv"

    val sparkBuilder = SparkSession
      .builder()
      .master("local[2]")
      .appName(appName)

    val spark = sparkBuilder.getOrCreate()

    val statisticsConfig = new StatisticsCoreConfig(spark)
    val nationStatistics = new NationStatistics(spark)

    println(statisticsConfig)
    println(nationStatistics)

    nationStatistics.buildReport(statisticsConfig, heartDiseaseMortalityDataCountyFilePath)
    spark.stop()
  }
}
