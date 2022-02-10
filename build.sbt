name := "learning-spark"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "io.delta" %% "delta-core" % "1.1.0",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)