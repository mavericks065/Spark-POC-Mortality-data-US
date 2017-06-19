package com.ng.poc.spark.mortality.datatype

case class BaseRecord(val year: Int, val locationState: String, val location: String, val geographicLevel: String,
                      val dataSource: String, val diseaseClass: String, val topic: String, val numberOfDead: Double,
                      val unit: String, val dataType: String, val dataSymbol: String, val dataStrat: String,
                      val genderCategory: String, val gender: String, val raceCategory: String, val race: String,
                      val topicId: String, val zipCode: Int, val coordinates: String)
