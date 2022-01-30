package com.jdreyesp.learningspark.chapter5

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.sql.functions.{col, expr}

object UnionsJoinsWindowingDF extends App with SparkSessionInitializer {

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

}
