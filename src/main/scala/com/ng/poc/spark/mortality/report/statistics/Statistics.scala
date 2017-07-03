package com.ng.poc.spark.mortality.report.statistics

import collection.mutable.Map
import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import org.apache.spark.sql.Dataset

trait Statistics {
  def runStats(statisticsConfig: StatisticsCoreConfig, file: String): Map[String, Dataset[Record]]

  val filterRecordsByGeographicLvl : (BaseRecord, String) => Boolean = {
    (data, filter) => data.geographicLevel == filter
  }

  val convertBaseRecordToRecord = (baseRecord: BaseRecord) => new Record(baseRecord.year, baseRecord.locationState,
    baseRecord.location, baseRecord.geographicLevel, baseRecord.numberOfDead,  baseRecord.gender, baseRecord.race)
}
