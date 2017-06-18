package com.ng.poc.spark.mortality.datatype

case class BaseRecord(year: Int, locationState: String, location: String, geographicLevel: String, dataSource: String,
                      diseaseClass: String, topic: String, numberOfDead: String, unit: String, dataType: String,
                      dataSymbol: String, dataStrat: String, genderCategory: String, gender: String,
                      raceCategory: String, race: String, topicId: String, zipCode: Int, coordinates: String)
