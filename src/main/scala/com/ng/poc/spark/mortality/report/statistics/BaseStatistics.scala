package com.ng.poc.spark.mortality.report.statistics

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

object BaseStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(BaseStatistics.getClass);
}

class BaseStatistics(sparkSession: SparkSession) {
  def buildReport(statisticsConfig: StatisticsCoreConfig) = ???


}
