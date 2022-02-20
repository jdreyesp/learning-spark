package com.jdreyesp.learningspark.chapter8

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, functions}

/**
 * Run `nc -lk 9999` before running this spark-submit job
 */
object SSSCount extends App with SparkSessionInitializer {

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  val words: DataFrame = lines.select(functions.split(col("value"), "\\s").alias("word"))
  val counts: DataFrame = words.groupBy("word").count()

  val streamingQuery =
    counts.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", "checkpoint/")
      .start()

  streamingQuery.awaitTermination()

}
