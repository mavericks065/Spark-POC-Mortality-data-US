package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.BaseRecord
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.spark.sql.{Encoders, _}

class StatisticsCoreConfig(sparkSession: SparkSession) extends Serializable {
  def getBaseDataSet = (heartDiseaseMortalityDataCountyFilePath: String) =>
    SparkReadWriteUtil.readCSVLocal(sparkSession, Encoders.product[BaseRecord], heartDiseaseMortalityDataCountyFilePath)
}
