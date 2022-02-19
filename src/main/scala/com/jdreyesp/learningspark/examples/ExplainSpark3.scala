package com.jdreyesp.learningspark.examples

import com.jdreyesp.learningspark.SparkSessionInitializer

object ExplainSpark3 extends App with SparkSessionInitializer {

  val moreOrdersDF = spark.range(1)

  println("Spark 3.X+ explain modes")

  println("SIMPLE EXPLAIN")
  moreOrdersDF.explain("simple")

  println("EXTENDED EXPLAIN")
  moreOrdersDF.explain("extended")

  println("CODEGEN EXPLAIN")
  moreOrdersDF.explain("codegen")

  println("COST EXPLAIN")
  moreOrdersDF.explain("cost")

}
