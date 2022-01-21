package com.jdreyesp.learningspark.chapter4

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, desc, expr, map_values, when}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

/**
 * Note: Using SF Fire Department dataset from chapter 3 instead of Airline On-Time Performance and Causes of Flight Delays
 * one, since it's not available through its website (https://oreil.ly/gfzLZ) at the time this exercise was implemented.
 */
object SFFireDepartmentSQL extends App {

  val spark = SparkSession.builder()
    .appName("SF Fire Department SQL")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .master("local")
    .getOrCreate()

  val csvFilePath: String = getClass.getClassLoader.getResource("chapter4/sf-fire-calls.csv").getPath

  // Using Data Definition Language in this case to show another way of defining the schema
  val schemaDDL =
    """
      |CallNumber INT,
      |UnitID STRING,
      |IncidentNumber BIGINT,
      |CallType STRING,
      |CallDate DATE,
      |WatchDate DATE,
      |CallFinalDisposition STRING,
      |AvailableDtTm TIMESTAMP,
      |Address STRING,
      |City STRING,
      |Zipcode STRING,
      |Battalion STRING,
      |StationArea INT,
      |Box INT,
      |OriginalPriority INT,
      |Priority INT,
      |FinalPriority INT,
      |ALSUnit BOOLEAN,
      |CallTypeGroup STRING,
      |NumAlarms INT,
      |UnitType STRING,
      |UnitSequenceInCallDispatch INT,
      |FirePreventionDistrict INT,
      |SupervisorDistrict INT,
      |Neighborhood STRING,
      |Location STRING,
      |RowID STRING,
      |Delay DOUBLE
      |""".stripMargin

  val sfFireDF = spark.read
    .option("header", true)
    .schema(schemaDDL)
    .csv(csvFilePath)

  println("Read and create a temporary view in Spark's default database")
  sfFireDF.createOrReplaceTempView("sf_fire_calls_tbl")

  spark.sql(
    """SELECT IncidentNumber, City, Delay
      |FROM sf_fire_calls_tbl WHERE Delay > 3
      |ORDER BY Delay DESC
      |""".stripMargin)
    .show(10, false)

  // Alternative with Dataframe API
  sfFireDF
    .select("IncidentNumber", "City", "Delay")
    .where("Delay > 3")
    .orderBy(desc("Delay"))
    .show(10, false)

  println("Using human-readable labels in a new column called Incident_Delay")
  spark.sql(
    """
      |SELECT IncidentNumber, City, Delay,
      |CASE
      | WHEN Delay > 100 THEN 'Very Long Delay'
      | WHEN Delay > 50 AND Delay <= 100 THEN 'Long Delay'
      | WHEN Delay > 10 AND Delay <= 50 THEN 'Medium Delay'
      | WHEN Delay > 0 AND Delay <= 10 THEN 'Tolerable Delay'
      | WHEN Delay = 0 THEN 'No Delay'
      | ELSE 'Early'
      |END AS Incident_Delay
      |FROM sf_fire_calls_tbl
      |ORDER BY IncidentNumber, Delay DESC
      |""".stripMargin)
    .show(10, false)

  spark.createDataFrame(sfFireDF
    .select(col("IncidentNumber"), col("City"), col("Delay"), col("Delay").as("Incident_Delay"))
    .where(col("IncidentNumber").isNotNull)
    .where(col("City").isNotNull)
    .where(col("Delay").isNotNull)
    .rdd.map { delay => delay match {
      case Row(in: Long, city: String, delay: Double, incident_delay: Double) if incident_delay > 100 => Row(in, city, delay, "Very Long Delay")
      case Row(in: Long, city: String, delay: Double, incident_delay: Double) if incident_delay > 50 && incident_delay <= 100 => Row(in, city, delay, "Long Delay")
      case Row(in: Long, city: String, delay: Double, incident_delay: Double) if incident_delay > 10 && incident_delay <= 50 => Row(in, city, delay, "Medium Delay")
      case Row(in: Long, city: String, delay: Double, incident_delay: Double) if incident_delay > 0 && incident_delay <= 10 => Row(in, city, delay, "Tolerable Delay")
      case Row(in: Long, city: String, delay: Double, incident_delay: Double) if incident_delay == 0 => Row(in, city, delay, "No Delay")
      case Row(in: Long, city: String, delay: Double, _: Double) => Row(in, city, delay, "Early")
    }
  }, StructType(Array(
    StructField("IncidentNumber", LongType, true),
    StructField("City", StringType, true),
    StructField("Delay", DoubleType, true),
    StructField("Incident_Delay", StringType, true)
  )))
    .orderBy(desc("Delay"), desc("IncidentNumber"))
    .show(10, false)

  // More efficient (no need to transform from/to rdd & no need to recheck schema)
  sfFireDF
    .select("IncidentNumber", "City", "Delay")
    .where(col("IncidentNumber").isNotNull)
    .where(col("City").isNotNull)
    .where(col("Delay").isNotNull)
    .withColumn("Incident_Delay",
      when(col("Delay") > 100, "Very Long Delay")
        .when(col("Delay") > 50 && col("Delay") <= 100, "Long Delay")
        .when(col("Delay") > 10 && col("Delay") <= 50, "Medium Delay")
        .when(col("Delay") > 0 && col("Delay") <= 10, "Tolerable Delay")
        .when(col("Delay") === 0, "No Delay")
        .otherwise("Early"))
    .orderBy(desc("Delay"), desc("IncidentNumber"))
    .show(10, false)

}
