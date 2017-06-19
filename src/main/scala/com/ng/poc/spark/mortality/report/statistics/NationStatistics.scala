package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}

object NationStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(NationStatistics.getClass);
  private val nation = "Nation"
  private val nationOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/nationOutputFile.csv"
}

class NationStatistics(sparkSession: SparkSession) {


  def buildReport(statisticsConfig: StatisticsCoreConfig, file: String): Unit = {
    NationStatistics.logger.info("Build report of " + file)

    val baseDataSet = statisticsConfig.getBaseDataSet(file)

    val nationDs = getNationDataSet(baseDataSet)

    SparkReadWriteUtil.writeReport(nationDs, NationStatistics.nationOutputFilePath)
  }

  def getNationDataSet(dataset: Dataset[BaseRecord]) : Dataset[Record] = {
    import sparkSession.implicits._
    dataset.filter(data => data.geographicLevel == NationStatistics.nation).as[Record]
  }
}
