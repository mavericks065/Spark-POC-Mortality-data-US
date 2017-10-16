package com.ng.poc.spark.mortality.util

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

trait SparkSessionProvider extends Specification with AfterAll {

  val logger = LoggerFactory.getLogger(classOf[SparkSessionProvider])

  logger.info("Creating SparkSession for tests")

  implicit val spark = SparkSession
    .builder()
    .appName(getClass.getName)
    .master("local[*]")
    .getOrCreate()

  def afterAll = {
    logger.info("Tests SparkSession stopped")
    spark.stop()
  }

}
