package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.BaseRecord
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.spark.sql.{Dataset, Encoders, _}

class StatisticsCoreConfig(sparkSession: SparkSession) extends Serializable {

  def getBaseDataSet(heartDiseaseMortalityDataCountyFilePath: String): Dataset[BaseRecord] = {
    SparkReadWriteUtil.readCSVLocal(sparkSession, Encoders.product[BaseRecord], heartDiseaseMortalityDataCountyFilePath)
  }

  def buildBaseDataFrame(baseDataSet: Dataset[BaseRecord]): DataFrame = {
    val filterExpression = (config: BaseRecord) => true

    val col = Seq("year", "locationState", "location", "geographicLevel", "numberOfDead", "gender", "race", "zipCode")
      .map(f => baseDataSet.col(f));

    getDataFrame(baseDataSet, col, filterExpression)
  }

  private def getDataFrame[T](dataset: Dataset[T], selectColumnsNames: Seq[Column], filterExpression:(T) => Boolean) : DataFrame = {
    dataset.filter(filterExpression).select(selectColumnsNames:_*)
  }

}
