package com.ng.poc.spark.mortality.report.statistics

import com.ng.poc.spark.mortality.datatype.BaseRecord
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.spark.sql.{Dataset, Encoders, _}

class StatisticsCoreConfig(sparkSession: SparkSession) extends Serializable {

  val heartDiseaseMortalityDataCountyFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US.csv"

  def getBaseDataFrame(): DataFrame = {
    val baseDataSet = SparkReadWriteUtil.readCSVLocal(sparkSession, Encoders.product[BaseRecord], heartDiseaseMortalityDataCountyFilePath)

    buildBaseDataFrame(baseDataSet)
  }

  def buildBaseDataFrame(baseDataSet: Dataset[BaseRecord]): DataFrame = {
    val filterExpression = (config: BaseRecord) => true

    val col = Seq("LocationAbbr", "LocationDesc", "GeographicLevel", "Data_Value", "Stratification1", "Stratification2")
      .map(f => baseDataSet.col(f));

    getDataFrame(baseDataSet, col, filterExpression)
  }

  def getDataFrame[T](dataset: Dataset[T], selectColumnsNames: Seq[Column], filterExpression:(T) => Boolean) : DataFrame = {
    dataset.filter(filterExpression).select(selectColumnsNames:_*)
  }

}
