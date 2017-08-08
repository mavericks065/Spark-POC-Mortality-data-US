package com.ng.poc.spark.mortality.run

import com.ng.poc.spark.mortality.report.statistics.{NationStatistics, StateDetailedStatistics, StatisticsCoreConfig}
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

object Main {

  val appName = "Mortality in the US report"
  val logger = LoggerFactory.getLogger(Main.getClass)
  val nationTable = "nation_statistics"
  val stateTable = "state_statistics"
  val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/Spark-POC-Mortality-data-US.csv"

  def main(args: Array[String]) {
    val sparkBuilder = SparkSession
      .builder()
      .master("local[2]")
      .appName(appName)

    val spark = sparkBuilder.getOrCreate()

    val statisticsConfig = new StatisticsCoreConfig(spark)
    val nationStatistics = new NationStatistics(spark)
    val stateStatistics = new StateDetailedStatistics(spark)

    val nationResults = nationStatistics.runStats(statisticsConfig, heartDiseaseMortalityDataCountyFilePath)
    val stateResults = stateStatistics.runStats(statisticsConfig, heartDiseaseMortalityDataCountyFilePath)

    for ((key, value) <- nationResults) {
      SparkReadWriteUtil.save(value, nationTable)
      SparkReadWriteUtil.writeReport(value, key)
    }
    for ((key, value) <- stateResults) {
      SparkReadWriteUtil.save(value, stateTable)
      SparkReadWriteUtil.writeReport(value, key)
    }

    spark.stop()
  }
}
