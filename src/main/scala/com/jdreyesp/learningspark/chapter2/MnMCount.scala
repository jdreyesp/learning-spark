package com.jdreyesp.learningspark.chapter2

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.sql.functions.{col, desc}

object MnMCount extends App with SparkSessionInitializer {

  if (args.length < 1) {
    print("Usage: MnMcount <mnm_file_dataset>")
    sys.exit(1)
  }

  // Get the M&M data set filename
  val nmnFile = args(0)

  // Read the file into a Spark Dataframe
  val mnmDF = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(nmnFile)

  // Alternative solution
  //  val groupedByStateAndColor = mnmDF.groupBy("State", "Color").sum("Count")
  //  val sortedDF = groupedByStateAndColor.sort(col("sum(count)").desc, col("State"))
  //
  //  sortedDF.show(100, false)

  val countMnMDF = mnmDF
    .select("State", "Color", "Count")
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy(desc("sum(Count)"))

  countMnMDF.show(60)
  println(s"Total rows = ${countMnMDF.count()}")

  val caCountMnNDF = mnmDF
    .select("State", "Color", "Count")
    .where(col("State") === "CA")
    .groupBy("State", "Color")
    .sum("Count")
    .select(col("State"), col("Color"), col("sum(Count)").as("Count"))
    .orderBy(desc("Count"))

  caCountMnNDF.show(10)

  spark.stop()
}
