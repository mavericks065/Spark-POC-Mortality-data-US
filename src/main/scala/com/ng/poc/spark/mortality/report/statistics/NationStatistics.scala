package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.Map

object NationStatistics extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(NationStatistics.getClass);
  private val nation = "Nation"
  private val nationOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/nationOutputFile"
}

class NationStatistics(sparkSession: SparkSession) extends Statistics with Serializable {

  override
  def runStats(statisticsConfig: StatisticsCoreConfig, file: String): Map[String, Dataset[Record]] = {
    NationStatistics.logger.info("Build report of " + file)

    val nationDs = statisticsConfig.getBaseDataSet.andThen(getNationDataSet).apply(file)

    Map(NationStatistics.nationOutputFilePath -> nationDs)
  }

  val getNationDataSet = (dataset: Dataset[BaseRecord]) => {
    import sparkSession.implicits._
    dataset.filter(filterRecordsByGeographicLvl(_, NationStatistics.nation)).map(convertBaseRecordToRecord)
  }
}
