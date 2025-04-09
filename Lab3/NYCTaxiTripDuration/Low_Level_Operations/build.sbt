name := "NYCTaxiTripDuration"

version := "1.10.11"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql" % "3.5.5",
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.4",
  "org.apache.hadoop" % "hadoop-client-runtime" % "3.3.4"
)