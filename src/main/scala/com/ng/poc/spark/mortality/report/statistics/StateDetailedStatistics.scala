package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.Map

object StateDetailedStatistics extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(NationStatistics.getClass);
  private val state = "State"
  private val stateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/stateOutputFile"
  private val overallStateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/overallStateOutputFile"
  private val maleStateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/maleStateOutputFile"
  private val femaleStateOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/femaleStateOutputFile"
  private val bestStateRatesOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/bestStateRatesOutputFile"
  private val overall = "Overall"
}
class StateDetailedStatistics(sparkSession: SparkSession) extends Statistics with Serializable {

  override
  def runStats(statisticsConfig: StatisticsCoreConfig, file: String): Map[String, Dataset[Record]] = {
    StateDetailedStatistics.logger.info("Build report of " + file)


    val stateDs = statisticsConfig.getBaseDataSet.andThen(getStateDataSet).apply(file)
    val filteredStateDs = filterDSByRaceAndGender(stateDs, StateDetailedStatistics.overall, StateDetailedStatistics.overall)
    val (overallDs, maleDs, femaleDs) = getDataByGenderIndependentlyOfRace(stateDs)

    val avgNumberOfDeadPerYear = getAvgNumberOfDeadPerYear(filteredStateDs)
    val bestStateRatesDs = getBestStateRates(filteredStateDs, avgNumberOfDeadPerYear)

    Map(StateDetailedStatistics.stateOutputFilePath -> stateDs,
      StateDetailedStatistics.overallStateOutputFilePath -> overallDs,
      StateDetailedStatistics.maleStateOutputFilePath-> maleDs,
      StateDetailedStatistics.femaleStateOutputFilePath -> femaleDs,
      StateDetailedStatistics.bestStateRatesOutputFilePath-> bestStateRatesDs
    )
  }

  private val getStateDataSet = (dataset: Dataset[BaseRecord]) => {
    import sparkSession.implicits._
    dataset.filter(filterRecordsByGeographicLvl(_, StateDetailedStatistics.state)).map(convertBaseRecordToRecord)
  }

  private val filterDSByRaceAndGender : (Dataset[Record], String, String) => Dataset[Record] = (dataset, race, gender) => {
    val filter : (Record, String, String) => Boolean = (record, race, gender) => {record.race == race && record.gender == gender}
    dataset.filter(filter(_, race, gender))
  }

  private val getDataByGenderIndependentlyOfRace : (Dataset[Record]) => (Dataset[Record], Dataset[Record], Dataset[Record]) = dataset => {
    (dataset.filter(record => record.race == StateDetailedStatistics.overall && record.gender == StateDetailedStatistics.overall),
      dataset.filter(record => record.race == StateDetailedStatistics.overall && record.gender == "Male"),
      dataset.filter(record => record.race == StateDetailedStatistics.overall && record.gender == "Female"))
  }

  private val getAvgNumberOfDeadPerYear : (Dataset[Record]) => Map[Int, Double] = dataset => {
    import sparkSession.implicits._
    val array: mutable.WrappedArray[(Int, Double)] = dataset.groupBy("year").avg("numberOfDead").map(convertRowToTuple)
      .collect()
    Map(array:_*)
  }

  private val getBestStateRates : (Dataset[Record], Map[Int, Double]) => Dataset[Record] = (dataset, map) => {
    val filter : (Record, Map[Int, Double]) => Boolean = (record, map) => record.numberOfDead > map(record.year)
    import org.apache.spark.sql.functions.desc
    dataset.filter(filter(_, map)).orderBy(desc("year"), desc("numberOfDead"))
  }

  private val convertRowToTuple : (Row) => (Int, Double) = row => (row.getAs[Int]("year"), row.getAs[Double]("avg(numberOfDead)"))
}
