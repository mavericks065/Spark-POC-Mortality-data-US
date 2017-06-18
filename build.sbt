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
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.8.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.8.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in assembly := Some("com.ng.poc.spark.mortality.run.Main")

scalacOptions in Test ++= Seq("-Yrangepos")
