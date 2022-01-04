package com.jdreyesp.learningspark

import org.apache.spark.sql.SparkSession

trait SparkProvider {

  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

}
