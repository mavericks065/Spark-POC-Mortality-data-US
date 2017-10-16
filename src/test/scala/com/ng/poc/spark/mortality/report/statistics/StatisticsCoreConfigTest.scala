package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.util.SparkSessionProvider

class StatisticsCoreConfigTest extends SparkSessionProvider {

  "The function get data frame" should {
    "return only the record fields" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/test_data.csv"
      val statisticsCore = new StatisticsCoreConfig(spark)

      val resultDataSet = statisticsCore.getBaseDataSet(heartDiseaseMortalityDataCountyFilePath)

      resultDataSet.count() must_== 63
      resultDataSet.collectAsList().get(0).year must_== 2013
    }
  }
}
