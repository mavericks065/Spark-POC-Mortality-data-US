package com.ng.poc.spark.mortality.report.statistics

import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

class NationStatisticsTest extends Specification {

  val spark = SparkSession
    .builder()
    .appName("StatisticsCoreConfigTest")
    .master("local[2]")
    .getOrCreate()

  "The function get nation dataset" should {
    "return only a dataset of Records with only Nation geographis lvls" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County.csv"
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
