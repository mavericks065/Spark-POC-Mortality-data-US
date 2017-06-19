package com.ng.poc.spark.mortality.util

import java.nio.file.{Files, Paths}

import com.ng.poc.spark.mortality.report.statistics.StatisticsCoreConfig
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

class SparkReadWriteUtilTest extends Specification {
  val spark = SparkSession
    .builder()
    .appName("StatisticsCoreConfigTest")
    .master("local[2]")
    .getOrCreate()

  "The function write report" should {
    "write a report" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County.csv"
      val outputFile = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/outputFile"
      val statisticsCore = new StatisticsCoreConfig(spark)
      val dataSet = statisticsCore.getBaseDataSet(heartDiseaseMortalityDataCountyFilePath)

      SparkReadWriteUtil.writeReport(dataSet, outputFile)

      Files.exists(Paths.get(outputFile)) must_== true
    }
  }
}
