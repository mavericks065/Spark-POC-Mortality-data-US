package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}

object NationStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(NationStatistics.getClass);
  private val nation = "Nation"
  private val nationOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/nationOutputFile"
}

class NationStatistics(sparkSession: SparkSession) extends Serializable {


  def buildReport(statisticsConfig: StatisticsCoreConfig, file: String): Unit = {
    NationStatistics.logger.info("Build report of " + file)

    val nationDs = statisticsConfig.getBaseDataSet.andThen(getNationDataSet).apply(file)

    SparkReadWriteUtil.writeReport(nationDs, NationStatistics.nationOutputFilePath)

  }

  def getNationDataSet = (dataset: Dataset[BaseRecord]) => {
    import sparkSession.implicits._
    dataset.filter(filterNationRecords).map(convertBaseRecordToRecord)
  }

  def filterNationRecords: (BaseRecord) => Boolean = {
    data => data.geographicLevel == NationStatistics.nation
  }

  def convertBaseRecordToRecord = (baseRecord: BaseRecord) => new Record(baseRecord.year, baseRecord.locationState,
    baseRecord.location, baseRecord.geographicLevel, baseRecord.numberOfDead,  baseRecord.gender, baseRecord.race)

}
