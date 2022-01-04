name := "learning-spark"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0" % Test,
  "org.apache.spark" %% "spark-sql" % "3.2.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)