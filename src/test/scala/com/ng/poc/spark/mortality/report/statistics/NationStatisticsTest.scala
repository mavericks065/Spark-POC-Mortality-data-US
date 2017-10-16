package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.util.SparkSessionProvider

class NationStatisticsTest extends SparkSessionProvider {

  "The function run stats" should {
    "return only a dataset of Records with only Nation geographis lvls" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/test_data.csv"
      val expectedKey = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/nationOutputFile"
      val statisticsCore = new StatisticsCoreConfig(spark)
      val nationStatistics = new NationStatistics(spark)

      val result = nationStatistics.runStats(statisticsCore, heartDiseaseMortalityDataCountyFilePath)

      result.size must_== 1
      result.get(expectedKey).get.count() must_== 3
      val nationList = result.get(expectedKey).get.collectAsList()
      nationList.get(0).numberOfDead must_== 328.4
      nationList.get(0).geographicLevel must_== "Nation"
    }
  }
}
