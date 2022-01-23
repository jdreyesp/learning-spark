package com.jdreyesp.learningspark.chapter3

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object SFFireDepartment extends App with SparkSessionInitializer {

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

  println("Which neighborhoods had the worst response times to fire calls in 2018?")
  fireTsDF
    .withColumn("DelayNumber", $"Delay".cast(FloatType))
    .drop("Delay")
    .where(year($"IncidentDate") === 2018)
    .groupBy("City", "Neighborhood")
    .max("DelayNumber")
    .select("City", "Neighborhood", "max(DelayNumber)")
    .orderBy(desc("max(DelayNumber)"))
    .show(10, false)

  println("Which week in the year 2018 had the most fire calls?")
  fireTsDF
    .where(year($"IncidentDate") === 2018)
    .groupBy(weekofyear($"IncidentDate").alias("week"))
    .count()
    .orderBy(desc("count"))
    .show(1)

  println("Is there a correlation between neighborhood, zip code, and number of fire calls?")
  fireTsDF
    .groupBy("City", "Neighborhood", "Zipcode")
    .count()
    .orderBy(desc("count"))
    .show(100, false)

  println("How can we use Parquet files or SQL tables to store this data and read it back?")
  fireTsDF
    .write
    .mode(SaveMode.Overwrite)
    .parquet("out/fireDpt")

  fireTsDF
    .withColumn("IncidentYear", year($"IncidentDate"))
    .withColumn("IncidentMonth", month($"IncidentDate"))
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("IncidentYear", "IncidentMonth")
    .parquet("out/fireDptPartitioned")

  spark.read.parquet("out/fireDpt").show(10, false)
  spark.read.parquet("out/fireDptPartitioned/IncidentYear=2018/IncidentMonth=1").show(10, false)
  spark.close()
}
