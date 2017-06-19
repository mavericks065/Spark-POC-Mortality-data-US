package com.ng.poc.spark.mortality.util

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object SparkReadWriteUtil {
  def readCSVLocal[T](sparkSession: SparkSession, encoder: Encoder[T], filePath: String): Dataset[T] = {
    sparkSession.read
      .option("delimiter", ",")
      .option("parserLib", "univocity")
      .option("header", true)
      .option("nullValue", "NULL")
      .option("mode", "FAILFAST")
      .schema(encoder.schema).csv(filePath).as(encoder)
  }

  def writeReport[T](dataset: Dataset[T], outputPath: String): Unit = {
    dataset.write
      .option("nullValue", "NULL")
      .option("delimiter", ",")
      .csv(outputPath)
  }
}
