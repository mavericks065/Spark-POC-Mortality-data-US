name := "Spark-POC-Mortality-data-US"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.specs2" %% "specs2-core" % "3.8.6" % "test")
libraryDependencies += "org.specs2" % "specs2-junit_2.11" % "3.8.6" % "test"
libraryDependencies += "org.specs2" % "specs2-scalacheck_2.11" % "3.8.6" % "test"
libraryDependencies += "org.specs2" % "specs2-matcher-extra_2.11" % "3.8.6" % "test"
libraryDependencies += "org.specs2" % "specs2-mock_2.11" % "3.8.6" % "test"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.25" % "test"
libraryDependencies += "org.postgresql" % "postgresql" % "42.1.1"

coverageEnabled in Test := true
coverageMinimum := 70
coverageFailOnMinimum := true
parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in assembly := Some("com.ng.poc.spark.mortality.run.Main")

scalacOptions in Test ++= Seq("-Yrangepos")
