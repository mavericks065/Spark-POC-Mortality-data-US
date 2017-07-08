package com.ng.poc.spark.mortality.report.statistics

import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

class StatisticsCoreConfigTest extends Specification with AfterAll {

  val spark = SparkSession
    .builder()
    .appName("StatisticsCoreConfigTest")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

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
