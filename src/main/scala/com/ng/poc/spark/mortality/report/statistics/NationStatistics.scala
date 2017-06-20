package com.ng.poc.spark.mortality.report.statistics

import java.util.Properties

import com.ng.poc.spark.mortality.datatype.{BaseRecord, Record}
import com.ng.poc.spark.mortality.util.SparkReadWriteUtil
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object NationStatistics extends Serializable {
  @transient lazy val logger = LogManager.getLogger(NationStatistics.getClass);
  private val nation = "Nation"
  private val nationOutputFilePath = "/Users/nicolasguignard-octo/Nicolas/priv_workspace/Spark-POC-Mortality-data-US/nationOutputFile.csv"
}

class NationStatistics(sparkSession: SparkSession) extends Serializable {


  def buildReport(statisticsConfig: StatisticsCoreConfig, file: String): Unit = {
    NationStatistics.logger.info("Build report of " + file)

    val baseDataSet = statisticsConfig.getBaseDataSet(file)

    val nationDs = getNationDataSet(baseDataSet)

    SparkReadWriteUtil.writeReport(nationDs, NationStatistics.nationOutputFilePath)

    save(nationDs)
  }

  def getNationDataSet(dataset: Dataset[BaseRecord]) : Dataset[Record] = {
    import sparkSession.implicits._
    dataset.filter(data => data.geographicLevel == NationStatistics.nation)
      .map(convertBaseRecordToRecord)
  }

  def convertBaseRecordToRecord(baseRecord: BaseRecord) : Record = {
    new Record(baseRecord.year, baseRecord.locationState, baseRecord.location, baseRecord.geographicLevel,
      baseRecord.numberOfDead,  baseRecord.gender, baseRecord.race)
  }

  def save(dataset: Dataset[Record]): Unit = {
    val host = "92.168.99.100"
    val user = "root"
    val pwd = "s3cretP4ssword123456789OverrideInPipeline"
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", pwd)
    connectionProperties.put("dirver", "postgresql")

    dataset.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
//      .option("driver", "postgresql")
      .jdbc("jdbc:postgresql://" + host, "record", connectionProperties)
  }
}
