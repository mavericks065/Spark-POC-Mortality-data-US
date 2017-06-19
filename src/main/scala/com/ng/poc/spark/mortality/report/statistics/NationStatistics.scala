package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import org.apache.logging.log4j.LogManager

import org.apache.spark.sql.{Dataset, SparkSession}

object NationStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(NationStatistics.getClass);
}

class NationStatistics(sparkSession: SparkSession) {
  def buildReport(statisticsConfig: StatisticsCoreConfig, file: String): Unit = {
    val baseDataSet = statisticsConfig.getBaseDataSet(file)

    val nationDs = getNationDataSet(baseDataSet)

  }

  def getNationDataSet(dataset: Dataset[BaseRecord]) : Dataset[Record] = {
    import sparkSession.implicits._
    dataset.filter(data => data.geographicLevel == "Nation").as[Record]
  }
}
