package com.ng.poc.spark.mortality.util

import java.nio.file.{Files, Paths}

import com.ng.poc.spark.mortality.report.statistics.StatisticsCoreConfig

import scala.reflect.io.{File, Path}
import scala.util.Try

class SparkReadWriteUtilTest extends SparkSessionProvider {

  val outputFile = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/outputFile"

  "The function write report" should {
    "write a report" in {
      val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/test_data.csv"
      val statisticsCore = new StatisticsCoreConfig(spark)
      val dataSet = statisticsCore.getBaseDataSet(heartDiseaseMortalityDataCountyFilePath)

      SparkReadWriteUtil.writeReport(dataSet, outputFile)

      Files.exists(Paths.get(outputFile)) must_== true
    }
  }

  override
  def afterAll = {
    super.afterAll
    val file : File = File (outputFile + ".csv")
    Try(file.delete)
    val path: Path = Path (outputFile)
    Try(path.deleteRecursively)
  }
}
