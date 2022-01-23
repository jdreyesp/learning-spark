package com.jdreyesp.learningspark

import org.apache.spark.sql.SparkSession

trait SparkSessionInitializer {

  val spark = SparkSession.builder()
    .appName("Learning spark")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .master("local")
    .getOrCreate()
}
