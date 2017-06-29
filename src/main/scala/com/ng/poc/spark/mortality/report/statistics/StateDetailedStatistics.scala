package com.ng.poc.spark.mortality.report.statistics

import com.google.common.collect.{ImmutableMap, ImmutableSet}
import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StateDetailedStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(NationStatistics.getClass);
  private val state = "State"
  private val stateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/stateOutputFile"
  private val overallStateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/overallStateOutputFile"
  private val maleStateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/maleStateOutputFile"
  private val femaleStateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/femaleStateOutputFile"
  private val bestStateRatesOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/bestStateRatesOutputFile"
}
class StateDetailedStatistics(sparkSession: SparkSession) extends Statistics with Serializable {

  override
  def runStats(statisticsConfig: StatisticsCoreConfig, file: String): Unit = {
    StateDetailedStatistics.logger.info("Build report of " + file)

    val stateDs = statisticsConfig.getBaseDataSet.andThen(getStateDataSet).apply(file)
    val filteredStateDs = filterDSByRaceAndGender(stateDs, "Overall", "Overall")
    val (overallDs, maleDs, femaleDs) = getDataByGenderInDifferentlyOfRace(stateDs)

    val avgNumberOfDeadPerYear = getAvgNumberOfDeadPerYear(filteredStateDs)
    val bestStateRatesDs = getBestStateRates(filteredStateDs, avgNumberOfDeadPerYear)

    SparkReadWriteUtil.writeReport(stateDs, StateDetailedStatistics.stateOutputFilePath)
    SparkReadWriteUtil.writeReport(overallDs, StateDetailedStatistics.overallStateOutputFilePath)
    SparkReadWriteUtil.writeReport(maleDs, StateDetailedStatistics.maleStateOutputFilePath)
    SparkReadWriteUtil.writeReport(femaleDs, StateDetailedStatistics.femaleStateOutputFilePath)
    SparkReadWriteUtil.writeReport(bestStateRatesDs, StateDetailedStatistics.bestStateRatesOutputFilePath)
  }

  val getStateDataSet = (dataset: Dataset[BaseRecord]) => {
    import sparkSession.implicits._
    dataset.filter(filterNationRecords(_, StateDetailedStatistics.state)).map(convertBaseRecordToRecord)
  }

  val filterDSByRaceAndGender : (Dataset[Record], String, String) => Dataset[Record] = (dataset, race, gender) => {
    dataset.filter(record => record.race == race && record.gender == gender)
  }

  val getDataByGenderInDifferentlyOfRace : (Dataset[Record]) => (Dataset[Record], Dataset[Record], Dataset[Record]) = dataset => {
    (dataset.filter(record => record.race == "Overall" && record.gender == "Overall"),
      dataset.filter(record => record.race == "Overall" && record.gender == "Male"),
      dataset.filter(record => record.race == "Overall" && record.gender == "Female"))
  }

  val getAvgNumberOfDeadPerYear : (Dataset[Record]) => Map[Int, Double] = dataset => {
    import sparkSession.implicits._
    Map(dataset.groupBy("year").avg("numberOfDead").map(convertRowToTuple)
      .collect():_*)
  }

  val getBestStateRates : (Dataset[Record], Map[Int, Double]) => Dataset[Record] = (dataset, map) => {
    val filter : (Record, Map[Int, Double]) => Boolean = (record, map) => record.numberOfDead > map(record.year)
    import org.apache.spark.sql.functions.desc
    dataset.filter(filter(_, map)).orderBy(desc("year"), desc("numberOfDead"))
  }

  val convertRowToTuple : (Row) => (Int, Double) = row => (row.getAs[Int]("year"), row.getAs[Double]("avg(numberOfDead)"))
}
