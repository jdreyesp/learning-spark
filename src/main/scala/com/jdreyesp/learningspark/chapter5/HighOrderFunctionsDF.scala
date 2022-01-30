package com.jdreyesp.learningspark.chapter5

import com.jdreyesp.learningspark.SparkSessionInitializer

object HighOrderFunctionsDF extends App with SparkSessionInitializer {

  //HOW TO DEAL WITH COMPLEX TYPES (arrays, maps)

  //Option 1: Explode and collect (not recommended since this can be cost efficient)
  /*
  -- In SQL
  SELECT id, collect_list(value + 1) AS values
  FROM (SELECT id, EXPLODE(values) AS value FROM table) x
  GROUP BY id
   */

  import spark.implicits._

  //Option 2: Using UDF (more recommended than option 1 but could be not efficient due to serdes)
  def toFahrenheit(values: Seq[Double]): Seq[Double] = values.map(t => {(t * 1.8) + 32})
  spark.udf.register("toFahrenheit", toFahrenheit(_: Seq[Double]): Seq[Double])

  val celsius: Seq[Double] = Seq(20.5,2,35.2,40.8,15)
  val df = Seq(celsius).toDF("celsius")
  df.createOrReplaceTempView("temperature_ex")
  spark.sql("SELECT celsius, toFahrenheit(celsius) AS fahrenheit FROM temperature_ex").show()

  //Option 3: Built-in functions for complex data types
  spark.sql("SELECT celsius, transform(celsius, t -> (t * 1.8) + 32) AS fahrenheit FROM temperature_ex").show()

  // Another example (hot places)
  spark.sql(
    """
        SELECT celsius,
          filter(celsius, t -> t > 38) as high,
          transform(celsius, t -> (t * 1.8) + 32) AS fahrenheit
        FROM temperature_ex
      """
  ).show()

  // and more: exists(), reduce()...

}
