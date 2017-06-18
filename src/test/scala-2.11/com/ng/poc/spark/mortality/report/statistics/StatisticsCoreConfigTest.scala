package com.ng.poc.spark.mortality.report.statistics

import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

class StatisticsCoreConfigTest extends Specification {

  val spark = SparkSession
    .builder()
    .appName("StatisticsCoreConfigTest")
    .master("local[2]")
    .getOrCreate()

  "The function get data frame" should {
    "return only the record fields" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County.csv"
      val statisticsCore = new StatisticsCoreConfig(spark)
      val resultDataFrame = statisticsCore getBaseDataFrame heartDiseaseMortalityDataCountyFilePath
      val firstRow = resultDataFrame head

      resultDataFrame.count() must_== 48
    }
  }
}
