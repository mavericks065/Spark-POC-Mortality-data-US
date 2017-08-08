package com.ng.poc.spark.mortality.util

import java.io.File
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

object SparkReadWriteUtil {
  def readCSVLocal[T](sparkSession: SparkSession, encoder: Encoder[T], filePath: String): Dataset[T] = {
    val options = Map(("delimiter", ","),
      ("parserLib", "univocity"),
      ("nullValue", "NULL"),
      ("mode", "FAILFAST"))
    sparkSession.read
      .options(options)
      .option("header", true)
      .schema(encoder.schema).csv(filePath).as(encoder)
  }

  def writeReport[T](dataset: Dataset[T], outputPath: String): Unit = {
    val options = Map(("nullValue", "NULL"), ("delimiter", ","))
    dataset.write
      .options(options)
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

  def save[T](dataset: Dataset[T], table: String): Unit = {
    val host = "192.168.99.100"
    val user = "root"
    val pwd = "password"
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", pwd)
    connectionProperties.put("ssl", "false")

    val url= "jdbc:postgresql://" + host + ":5432/sparkpoc"

    dataset.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("driver", "org.postgresql.Driver")
      .jdbc(url, table, connectionProperties)
  }
}
