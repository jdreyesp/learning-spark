package com.jdreyesp.learningspark.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, desc, expr, month, to_timestamp, year}

object SFFireDepartment extends App {

  val spark = SparkSession.builder()
    .appName("SF Fire Department")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .master("local")
    .getOrCreate()

  val sfFireDepartmentInputFilePath = getClass.getClassLoader.getResource("chapter3/sf-fire-calls.csv").getPath

  val fireDF = spark.read
    .option("samplingRatio", 0.001) //samplingRatio (default is 1.0): defines fraction of rows used for schema inferring.
    .option("header", true)
    .csv(sfFireDepartmentInputFilePath)

  println("Inferred schema by Spark:")
  fireDF.printSchema

  println("Using projection and filter")
  val newFireDF = fireDF
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") =!= "Medical Incident") //=!= inequality check that returns a Column

  newFireDF.show(5, false)

  println("Count all distinct from a column")
  val distinctCallTypesFireDF = fireDF
    .where(col("CallType").isNotNull) // Filtering nulls from a nullable column
    .agg(countDistinct("CallType").alias("DistinctCallTypes"))

  distinctCallTypesFireDF.show()

  println("Seeing each of the 32 distinct call types")
  val callTypesDF = fireDF
    .where(col("CallType").isNotNull)
    .distinct()

  callTypesDF.show()

  println("Renaming columns and using them in projections and filters")
  val renamedFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedInMins")
    //.select("ResponseDelayedInMins")
    .where(col("ResponseDelayedInMins") > 5)

  renamedFireDF.show(5, false)

  println("Using timestamp functions in Spark to convert String columns to Spark Timestamp columns")
  val fireTsDF = fireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm")

  fireTsDF
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, false)

  println("Extract year from Spark timestamp column")
  import spark.implicits._

  val fireYearTsDF = fireTsDF
    .select(year($"IncidentDate"))
    .distinct()
    .orderBy(year($"IncidentDate"))

  fireYearTsDF.show()

  println("Grouping and aggregating")
  val countFireTsDF = fireTsDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .groupBy("CallType")
    .count()
    .orderBy(desc("count"))

  countFireTsDF.show()

  println("Aggregating Functions")
  import org.apache.spark.sql.{functions => F} // Spark import aliasing functions to 'F'
  val aggFunctionsFireDF = renamedFireDF
    .select(F.sum("NumAlarms"), F.avg("ResponseDelayedInMins"),
      F.min("ResponseDelayedInMins"), F.max("ResponseDelayedInMins"))

  aggFunctionsFireDF.show()

  println("OTHER EXERCISES")

  println("What were all the different types of fire calls in 2018?")
  fireTsDF
    .where(year($"IncidentDate") === 2018)
    .select("CallType", "IncidentDate")
    .distinct()
    .show()


  println("What months within the year 2018 saw the highest number of fire calls?")
  fireTsDF
    .where(year($"IncidentDate") === 2018)
    .groupBy(month($"IncidentDate"))
    .count()
    .withColumnRenamed("count", "monthCount")
    .select("month(IncidentDate)", "monthCount")
    .orderBy(desc("monthCount"))
    .show(10, false)

  println("Which neighborhoods in San Francisco generated the most fire calls in 2018?")
  fireTsDF
    .where(year($"IncidentDate") === 2018)
    .where($"City" === "San Francisco")
    .groupBy("Neighborhood")
    .count()
    .orderBy(desc("count"))
    .select("Neighborhood", "count")
    .show(10, false)

  println("Which neighborhood had the worst response times to fire calls in 2018?")


  spark.close()
}
