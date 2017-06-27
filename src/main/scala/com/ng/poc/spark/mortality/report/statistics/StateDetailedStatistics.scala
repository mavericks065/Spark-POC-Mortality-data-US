package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}

object StateDetailedStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(NationStatistics.getClass);
  private val state = "State"
  private val stateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/stateOutputFile"
}
class StateDetailedStatistics(sparkSession: SparkSession) extends Statistics with Serializable {

  override
  def runStats(statisticsConfig: StatisticsCoreConfig, file: String): Unit = {
    StateDetailedStatistics.logger.info("Build report of " + file)

    val stateDs = statisticsConfig.getBaseDataSet.andThen(getStateDataSet).apply(file)

    SparkReadWriteUtil.writeReport(stateDs, StateDetailedStatistics.stateOutputFilePath)
  }

  val getStateDataSet = (dataset: Dataset[BaseRecord]) => {
    import sparkSession.implicits._
    dataset.filter(filterNationRecords(_, StateDetailedStatistics.state)).map(convertBaseRecordToRecord)
  }

  val getDataByGenderInDifferentlyOfRace : (Dataset[Record]) => (Dataset[Record], Dataset[Record], Dataset[Record]) = dataset => {
    (dataset.filter(record => record.race == "Overall" && record.gender == "Overall"),
      dataset.filter(record => record.race == "Overall" && record.gender == "Male"),
      dataset.filter(record => record.race == "Overall" && record.gender == "Female"))
  }
}
