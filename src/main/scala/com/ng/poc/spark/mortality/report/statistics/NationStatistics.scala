package com.ng.poc.spark.mortality.report.statistics

import java.util.Properties

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object NationStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(NationStatistics.getClass);
  private val nation = "Nation"
  private val nationOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/nationOutputFile"
}

class NationStatistics(sparkSession: SparkSession) extends Serializable {


  def buildReport(statisticsConfig: StatisticsCoreConfig, file: String): Unit = {
    NationStatistics.logger.info("Build report of " + file)

    val nationDs = statisticsConfig.getBaseDataSet.andThen(getNationDataSet).apply(file)

    SparkReadWriteUtil.writeReport(nationDs, NationStatistics.nationOutputFilePath)

    save(nationDs)
  }

  def getNationDataSet = (dataset: Dataset[BaseRecord]) => {
    import sparkSession.implicits._
    dataset.filter(filterNationRecords).map(convertBaseRecordToRecord)
  }

  def filterNationRecords: (BaseRecord) => Boolean = {
    data => data.geographicLevel == NationStatistics.nation
  }

  def convertBaseRecordToRecord = (baseRecord: BaseRecord) => new Record(baseRecord.year, baseRecord.locationState,
    baseRecord.location, baseRecord.geographicLevel, baseRecord.numberOfDead,  baseRecord.gender, baseRecord.race)


  def save(dataset: Dataset[Record]): Unit = {
    val host = "92.168.99.100"
    val user = "root"
    val pwd = "s3cretP4ssword123456789OverrideInPipeline"
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", pwd)
    connectionProperties.put("driver", "org.postgresql.Driver")
//    connectionProperties.put("stringtype", "unspecified")
    connectionProperties.put("ssl", "true")

    dataset.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
//      .option("driver", "postgresql")
      .jdbc("jdbc:postgresql://" + host + "/sparkpoc", "record", connectionProperties)
  }
}
