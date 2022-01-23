package com.jdreyesp.learningspark.chapter4

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame

import java.io.File

object DataSources extends App with SparkSessionInitializer {

  println("Reading parquet")

  import java.net.URLDecoder

  val specificParquetFileResource = ClassLoader.getSystemResource("chapter4/parquet/IncidentYear=2000/IncidentMonth=4/part-00000-8d0e4fb8-365b-4478-9f3e-521a13400991.c000.snappy.parquet")
  val specificParquetFile = URLDecoder.decode(specificParquetFileResource.getFile, "UTF-8")

  val allParquetFiles =
    getClass
      .getClassLoader
      .getResource("chapter4/parquet/")
      .getPath

  println(s"""Number of rows on specific parquet file: ${spark
    .read
    .format("parquet") // Not needed since parquet is the default format for DataFrameReader
    .load(specificParquetFile)
    .count()}""")

  println("Reading all parquet files")
  println(s"""Number of rows on all parquet files: ${spark
    .read
    .format("parquet")
    .load(allParquetFiles)
    .count()}""")

  println("Reading parquet as SQL table")
  spark.sql(
    s"""
      |CREATE OR REPLACE TEMPORARY VIEW sf_fire_incidents_parquet
      |USING parquet
      |OPTIONS(
      | path "$specificParquetFile"
      |)
      |""".stripMargin
  )
  val parquetDF: DataFrame = spark.table("sf_fire_incidents_parquet")
  parquetDF.show(10, false)

  println("Writing parquet as file")
  parquetDF
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .save("out/parquet-copy")

  println("Writing parquet as table")
  // Needed since from Spark 3.0 spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation=true cannot be set
  // REPLACE IT WITH LOCAL TARGET SPARK-WAREHOUSE DIRECTORY
  FileUtils
    .deleteDirectory(new File("/Users/juandiegoreyesponce/workspaces/jdreyesp/learning-spark/spark-warehouse/sf_fire_incidents_copy_parquet"))

  parquetDF
    .write
    .saveAsTable("sf_fire_incidents_copy_parquet")

  spark.table("sf_fire_incidents_copy_parquet").show(10, false)

  // Similar DataFrameReader / DataFrameWriter options for JSON, CSV, Avro, ORC, Images, Binary
}
