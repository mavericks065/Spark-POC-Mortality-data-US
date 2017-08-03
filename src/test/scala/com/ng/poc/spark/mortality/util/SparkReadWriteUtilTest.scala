package com.ng.poc.spark.mortality.util

import java.nio.file.{Files, Paths}

import com.ng.poc.spark.mortality.report.statistics.StatisticsCoreConfig

class SparkReadWriteUtilTest extends TestUtils {

  "The function write report" should {
    "write a report" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/test_data.csv"
      val outputFile = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/outputFile"
      val statisticsCore = new StatisticsCoreConfig(spark)
      val dataSet = statisticsCore.getBaseDataSet(heartDiseaseMortalityDataCountyFilePath)

      SparkReadWriteUtil.writeReport(dataSet, outputFile)

      Files.exists(Paths.get(outputFile)) must_== true
    }
  }
}
