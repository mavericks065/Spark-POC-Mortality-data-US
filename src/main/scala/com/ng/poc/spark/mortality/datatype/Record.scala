package com.ng.poc.spark.mortality.datatype

case class Record(val year: Int, val locationState: String, val location: String, val geographicLevel: String,
             val numberOfDead: Double, val gender: String, val race: String)
