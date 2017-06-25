package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}

trait Statistics {
  def runStats(statisticsConfig: StatisticsCoreConfig, file: String): Unit

  val filterNationRecords: (BaseRecord, String) => Boolean = {
    (data, filter) => data.geographicLevel == filter
  }

  val convertBaseRecordToRecord = (baseRecord: BaseRecord) => new Record(baseRecord.year, baseRecord.locationState,
    baseRecord.location, baseRecord.geographicLevel, baseRecord.numberOfDead,  baseRecord.gender, baseRecord.race)
}
