package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import com.ng.poc.spark.mortality.util.SparkSessionProvider
import org.apache.spark.sql.Dataset

class StatisticsTest extends SparkSessionProvider {

  "The function convert base record to record" should {
    "return record" in {
      val baseRecord = new BaseRecord(2013, "AK", "test", "Nation", "T", "T", "T", 147.9, "U", "dataType", "S", "S",
        "G", "Male", "RC", "Overall", "T", 12, "C")
      val expectedRecord = new Record(2013, "AK", "test", "Nation", 147.9, "Male", "Overall")
      val statistics = new Statistics {
        override
        def runStats(statisticsConfig: StatisticsCoreConfig, file: String): collection.mutable.Map[String, Dataset[Record]] = ???
      }

      val record = statistics.convertBaseRecordToRecord(baseRecord)

      record must_== expectedRecord
    }
  }
}
