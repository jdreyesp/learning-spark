package com.jdreyesp.learningspark.chapter9

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.io.File

object Delta extends App with SparkSessionInitializer {

  val baseDir = "/tmp/chapter9"

  val sourcePath = getClass.getClassLoader.getResource("chapter9/blogs/blogs.json").getPath
  val sourceStreamingPath1 = getClass.getClassLoader.getResource("chapter9/newBlogs/").getPath
  val checkpointLocation = s"$baseDir/checkpoint"

  val deltaPath = s"$baseDir/blogsdelta"

  val schema = StructType(
      List(
        StructField("Id", IntegerType, false),
        StructField("First", StringType, false),
        StructField("Last", StringType, false),
        StructField("Url", StringType, false),
        StructField("Published", StringType, false),
        StructField("Hits", IntegerType, false),
        StructField("Campaigns", ArrayType(StringType), false)
      )
  )

  FileUtils.deleteDirectory(new File(baseDir))

  /** BATCH */
  spark.read
    .format("json")
    .schema(schema)
    .load(sourcePath)
    .write
    .format("delta")
    .save(deltaPath)

  spark.read
    .format("delta")
    .load(deltaPath)
    .show(false)

  /** STREAMING */
  spark.readStream
    .schema(schema)
    .json(sourceStreamingPath1)
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpointLocation)
    .trigger(Trigger.Once())
    .start(deltaPath)
    .awaitTermination()

  spark.read
    .format("delta")
    .load(deltaPath)
    .filter(col("Id").isNotNull)
    .show(false)
}
