package com.jdreyesp.learningspark

import org.apache.spark.sql.SparkSession

trait SparkSessionInitializer {

  val spark = SparkSession.builder()
    .appName("Learning spark")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()
}
