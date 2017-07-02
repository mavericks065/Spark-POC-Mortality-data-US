package com.ng.poc.spark.mortality.report.statistics

import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

class StateDetailedStatisticsTest extends Specification {
  val spark = SparkSession
    .builder()
    .appName("StateStatisticsTest")
    .master("local[1]")
    .getOrCreate()

  val statisticsCore = new StatisticsCoreConfig(spark)
  val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/test_data.csv"
  val stateStatistics = new StateDetailedStatistics(spark)
  val baseDs = statisticsCore.getBaseDataSet(heartDiseaseMortalityDataCountyFilePath)

  "The function get state dataset" should {
    "return only a dataset of Records with only State geographis lvls" in {
      val result = stateStatistics.getStateDataSet(baseDs)

      result.count() must_== 9
      val nationList = result.collectAsList()
      nationList.get(0).numberOfDead must_== 269.3
      nationList.get(0).geographicLevel must_== "State"
    }
  }

  "The function get data by gender" should {
    "return an integer" in {
      val ds = stateStatistics.getStateDataSet(baseDs)
      val (overallDs, maleDs, femaleDs) = stateStatistics.getDataByGenderInDifferentlyOfRace(ds)

      overallDs.count() must_== 1
      femaleDs.count() must_== 1
      maleDs.count() must_== 1
    }
  }

  "The function filter dataset by race and gender" should {
    "return a dataset of Records with only the race and record specified" in {
      val ds = stateStatistics.getStateDataSet(baseDs)
      val result = stateStatistics.filterDSByRaceAndGender(ds, "Black", "Overall")

      result.count() must_== 1
    }
  }

  "The function get average number of dead per year" should {
    "return a map with one element in 2013 and value " in {
      val ds = stateStatistics.getStateDataSet(baseDs)
      val result = stateStatistics.getAvgNumberOfDeadPerYear(ds)

      result.size must_== 1
      result(2013) must_== 243.38888888888889
    }
  }
}
