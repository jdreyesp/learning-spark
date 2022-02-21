package com.jdreyesp.learningspark.examples.deltastreaming

import com.jdreyesp.learningspark.SparkSessionInitializer
import com.jdreyesp.learningspark.examples.deltastreaming.DeltaStreaming.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder}

import java.nio.file.{Files, Paths, StandardCopyOption}

case class Order(id: Int, name: String, quantity: Int) {
  require(quantity > 0)
}
case class OrderQuantity(name: String, count: Long)

/** Let's say I have 2 ETL jobs.
  - One will load new incoming data and put it in a delta lake
  - The other one will take the new adquired data (from the latest version of the delta lake table **/
object DeltaStreaming extends App with SparkSessionInitializer {

  import spark.implicits._

  val schema = ScalaReflection.schemaFor[Order].dataType.asInstanceOf[StructType]

  def transformOrder(ds: Dataset[Order]): Dataset[OrderQuantity] = {
    ds
      .groupBy("name")
      .count()
      .as[OrderQuantity]
  }

  JsonToConsoleETL.start(schema, transformOrder)

}

object JsonToConsoleETL {
  private val deltaTablesPath = "delta/streaming"
  private val inputData = s"$deltaTablesPath/data"
  private val checkpointLocationWriterStreaming = s"$deltaTablesPath/_checkpoints/writer"
  private val checkpointLocationReaderStreaming = s"$deltaTablesPath/_checkpoints/reader"

  Logger.getLogger("org").setLevel(Level.WARN)

  def start[U, V](jsonSchema: StructType, transformFn: Dataset[U] => Dataset[V])(implicit encoder: Encoder[U]): Unit = {

    Files.createDirectories(Paths.get(inputData))
    Files.copy(
      Paths.get(getClass.getClassLoader.getResource("examples/deltastreaming/warmup.json").getPath),
      Paths.get(s"$inputData/warmup.json"),
      StandardCopyOption.REPLACE_EXISTING)

    startWriterStream(jsonSchema)
    Thread.sleep(10000)
    startReaderStream(transformFn)

    spark.streams.awaitAnyTermination()
  }

  // Reads from deltalake table and prints to console the transformed data
  private def startReaderStream[D, R](transformFunction: Dataset[D] => Dataset[R])(implicit encoder: Encoder[D]): StreamingQuery = {
    val dsStream = spark.readStream
      .format("delta")
      .load(deltaTablesPath)
      .as[D]

    transformFunction(dsStream)
      .writeStream
      .option("checkpointLocation", checkpointLocationReaderStreaming)
      .queryName("DLReaderConsoleWriter")
      .outputMode("complete")
      .format("console")
      .start()
  }

  private def startWriterStream[U](jsonSchema: StructType)(implicit encoder: Encoder[U]): StreamingQuery = {

    val ordersDS = spark.readStream
      .format("json")
      .option("multiline", "true")
      .schema(jsonSchema)
      .load(inputData)
      .as[U]

    ordersDS
      .writeStream
      .queryName("JSONReaderDLWriter")
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationWriterStreaming)
      .start(deltaTablesPath)
  }

}
