package com.ng.poc.spark.mortality.report.statistics

import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

class NationStatisticsTest extends Specification with AfterAll {

  val spark = SparkSession
    .builder()
    .appName("NationStatisticsTest")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  "The function get nation dataset" should {
    "return only a dataset of Records with only Nation geographis lvls" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/test_data.csv"
      val statisticsCore = new StatisticsCoreConfig(spark)
      val nationStatistics = new NationStatistics(spark)

      val baseDs = statisticsCore.getBaseDataSet(heartDiseaseMortalityDataCountyFilePath)

      val result = nationStatistics.getNationDataSet(baseDs)

      result.count() must_== 3
      val nationList = result.collectAsList()
      nationList.get(0).numberOfDead must_== 328.4
      nationList.get(0).geographicLevel must_== "Nation"
    }
  }
}
