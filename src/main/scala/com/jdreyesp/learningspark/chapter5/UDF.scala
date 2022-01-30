package com.jdreyesp.learningspark.chapter5

import com.jdreyesp.learningspark.SparkSessionInitializer

object UDF extends App with SparkSessionInitializer {

  println("Defining and calling UDF from Spark SQL")
  val cubed = (s: Long) => s * s * s

  spark.udf.register("cubed", cubed)
  spark.range(1, 9).createOrReplaceTempView("udf_test")

  spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show(10, false)

  // Important note if defining PySpark UDFs: Pandas UDF (or vectorized UDFs) have been introduced from Spark 2.3
  // so that rows aren't processed one by one (known issue in Spark on slowness on data movement between JVM -> Python)
  // but processed as Pandas Dataframes. This speeds up and distribute data nicely.
  //
  // Just define the udf using `pandas_udf(...)`
  // More info: https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html

}
