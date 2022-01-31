package com.jdreyesp.learningspark.chapter5

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.sql.functions.{col, expr}

object OperationsDF extends App with SparkSessionInitializer {

  // #1. LOAD AND PREPARE DATA

  val delaysPath = getClass.getClassLoader.getResource("chapter5/departuredelays.csv").getPath
  val airportsPath = getClass.getClassLoader.getResource("chapter5/airport-codes-na.txt").getPath

  // Obtain airport information data set (look at the reading as csv format by spark)
  val airports = spark.read
    .option("header", true)
    .option("inferschema", true)
    .option("delimiter", "\t")
    .csv(airportsPath)

  airports.createOrReplaceTempView("airports_na")

  // Obtain departure delays data set
  val delays = spark.read
    .option("header", true)
    .csv(delaysPath)
    // Overwriting columns to apply to specific data type.
    // This could have been done with DDL or StructType schema definitions (as well as Dataset, but it's unnecessary overhead)
    .withColumn("delay", expr("CAST(delay as INT) as delay"))
    .withColumn("distance", expr("CAST(distance as INT) as distance"))

  delays.createOrReplaceTempView("departureDelays")

  // Create temporary small table. This generates a 3 row table that's more performant to process
  val delayFilter = """
        origin == 'SEA'
        AND destination == 'SFO'
        AND date like '01010%'
        AND delay > 0
        """

  val foo = delays.filter(expr(delayFilter))

  foo.createOrReplaceTempView("foo")

  spark.table("foo").show()

  // #2. UNION
  val bar = delays.union(foo)
  bar.createOrReplaceTempView("bar")
  bar.filter(expr(delayFilter)).show()

  // #3. JOIN
  //join options: https://oreil.ly/CFEhb
  foo.join(
    airports.as('air),
    col("air.IATA") === col("origin")
    // Inner join
  ).select("City", "State", "date", "delay", "distance", "destination").show()

  // #4. WINDOWING
  spark.sql(
    """
      SELECT origin, destination, SUM(delay) AS TotalDelays
      FROM departureDelays
      WHERE origin IN ('SEA', 'SFO', 'JKF')
      AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
      GROUP BY origin, destination
      """).createOrReplaceTempView("departureDelaysWindow")

  spark
    .sql(
    """
      SELECT origin, destination, TotalDelays, rank
      FROM (
        SELECT origin, destination, TotalDelays, dense_rank()
        OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
        FROM departureDelaysWindow
      ) t
      WHERE rank <= 3
      """).show()

  // #5. MODIFICATIONS

  // Adding columns
  val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
  foo2.show()

  // Dropping columns
  val foo3 = foo2.drop("delay")
  foo3.show()

  // Renaming column
  val foo4 = foo3.withColumnRenamed("status", "flight_status")
  foo4.show()

  // Pivoting columns
  // Pivoting allows renaming columns as well as performing aggregate calculations
  val foo5 = spark.sql(
    """
      SELECT * FROM (
      SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
      FROM departureDelays
      WHERE origin = 'SEA')
      PIVOT (
        CAST(AVG(delay) AS DECIMAL(4,2)) AS AvgDelay, MAX(delay) AS MaxDelay
        FOR month IN (1 JAN, 2 FEB)
      )
      ORDER BY destination
      """).show()


}
