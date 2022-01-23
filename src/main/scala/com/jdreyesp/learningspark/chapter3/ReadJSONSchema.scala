package com.jdreyesp.learningspark.chapter3

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, expr}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object ReadJSONSchema extends App {

  // We use conf here to show another way of initializing a SparkSession
  val conf = new SparkConf()
    .setAppName("ReadJSONSchema")
    .setMaster("local")
    .set("spark.driver.bindAddress", "127.0.0.1")
    .set("spark.driver.host", "127.0.0.1")

  val sparkConf = SparkContext.getOrCreate(conf)

  val spark = SparkSession
    .builder()
    .config(sparkConf.getConf)
    .getOrCreate()

  if (args.length <= 0) {
    println("usage ReadJSONSchema <file path to blogs.json>")
    System.exit(1)
  }

  // Get the path to the JSON file
  val jsonFile = args(0)

  // Define our schema programmatically
//  val schema = StructType(Array(
//    StructField("Id", IntegerType, false),
//    StructField("First", StringType, false),
//    StructField("Last", StringType, false),
//    StructField("Url", StringType, false),
//    StructField("Published", StringType, false),
//    StructField("Hits", IntegerType, false),
//    StructField("Campaigns", ArrayType(StringType), false)
//  ))

  // Define our schema using Spark DDL
  val schema = """
    Id INTEGER NOT NULL,
    First STRING NOT NULL,
    Last STRING NOT NULL,
    Url STRING NOT NULL,
    Published STRING NOT NULL,
    Hits STRING NOT NULL,
    Campaigns ARRAY<STRING> NOT NULL
    """

  // Create a dataframe by reading from the JSON file
  // with a predefined schema
  val blogsDF = spark.read.schema(schema).json(jsonFile)

  // Show the Dataframe schema as output
  blogsDF.show(false)

  // Prints the schema
  println(blogsDF.printSchema)
  println(blogsDF.schema)

  // Compute using spark functions
  println(blogsDF.col("Id"))

  println("SIMPLE ARITHMETIC FUNCTIONS")
  println(blogsDF.select(expr("Hits * 2")).show(2))
  println(blogsDF.select(col("Hits") * 2).show(2))

  println("BIG HITTERS")
  println(blogsDF.withColumn("Big Hitters", expr("Hits > 10000")).show())

  println("CONCATENATE COLUMNS")
  println(blogsDF
    .withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))
    .select(col("AuthorsId"))
    .show(4))

  println("EXPR SAME AS COL")
  println(blogsDF
  .select(expr("Hits")).show(2))
  println(blogsDF
    .select(col("Hits")).show(2))

  println("SORT")
  println(blogsDF.sort(col("Id").desc).show())

  println("USING ROWS")
  import scala.collection.JavaConverters._
  import spark.implicits._

  println("FIRST WAY")
  val seqRow: Seq[(String, String)] = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
  val authorsDF = seqRow.toDF("Author", "State")
  println(authorsDF.show())

  println("SECOND WAY (FORCING SCHEMA)")
  val rows: java.util.List[Row] = Seq(Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")).asJava
  val authorsDFSchema: DataFrame = spark.createDataFrame(rows, StructType(Array(StructField("Author", StringType, false), StructField("State", StringType, false))))
  println(authorsDFSchema.show())

  println("WRITE AUTHORS AS PARQUET, PARTITIONED BY STATE")
  authorsDFSchema.write
    .format("parquet")
    .partitionBy("State")
    .mode(SaveMode.Overwrite)
    .save("./out/authors")

  println(spark.read.parquet("./out/authors").show(false))

  spark.close()
}
