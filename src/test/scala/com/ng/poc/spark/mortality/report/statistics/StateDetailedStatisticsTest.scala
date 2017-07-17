package com.ng.poc.spark.mortality.report.statistics

import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

class StateDetailedStatisticsTest extends Specification with AfterAll {

  val spark = SparkSession
    .builder()
    .appName("StateStatisticsTest")
    .master("local[1]")
    .getOrCreate()

  val statisticsCore = new StatisticsCoreConfig(spark)
  val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/test_data.csv"
  val stateStatistics = new StateDetailedStatistics(spark)
  val baseDs = statisticsCore.getBaseDataSet(heartDiseaseMortalityDataCountyFilePath)

  override def afterAll(): Unit = {
    spark.stop()
  }

  "the function run stats of StateDetailedStatistics" should {
    val result = stateStatistics.runStats(statisticsCore, heartDiseaseMortalityDataCountyFilePath)
    "return state, overall, male, female and bestStateRates datasets" in {
      result.size must_== 5
    }
    "return only a dataset of Records with only State geographic lvls" in {
      val expectedStateKey = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/stateOutputFile"

      val stateListOfRecords = result.get(expectedStateKey).get.collectAsList()
      stateListOfRecords.get(0).numberOfDead must_== 269.3
      stateListOfRecords.get(0).geographicLevel must_== "State"
    }
    "return only a dataset of Records with only OVERALL sexes" in {
      val expectedStateKey = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/overallStateOutputFile"

      val stateListOfRecords = result.get(expectedStateKey).get.collectAsList()
      stateListOfRecords.size() must_== 1
    }
    "return only a dataset of Records with only MALE sexes" in {
      val expectedStateKey = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/maleStateOutputFile"

      val stateListOfRecords = result.get(expectedStateKey).get.collectAsList()
      stateListOfRecords.size() must_== 1
    }
    "return only a dataset of Records with only FEMALE sexes" in {
      val expectedStateKey = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/femaleStateOutputFile"

      val stateListOfRecords = result.get(expectedStateKey).get.collectAsList()
      stateListOfRecords.size() must_== 1
    }
  }

}
