package com.ng.poc.spark.mortality.util

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
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

    val path = new File(outputPath)
    val baseName = path.getName

    merge(dataset.columns.mkString(","), outputPath, outputPath + "/../" + baseName + ".csv")

  }

  def merge(headerRow: String, inputFolderPath: String, outputPath: String) = {
    val conf = new Configuration()
    val local = FileSystem.getLocal(conf)

    val out = local.create(new Path(outputPath))
    out.write((headerRow + "\n").getBytes("UTF-8"));
    val files = local.listStatus(new Path(inputFolderPath))
    for (fileStatus <- files) {
      val in = local.open(fileStatus.getPath())
      IOUtils.copyBytes(in, out, conf, false);
    }
    out.close()
  }
}
