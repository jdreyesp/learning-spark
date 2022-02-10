package com.jdreyesp.learningspark.chapter7

import org.apache.spark.sql.SparkSession

object SparkTuning extends App {

  // APPLYING CONF (CONFIGURATION SET IN ORDER OF APPEARANCE IN THIS LIST)

  // #1. By modifying spark-defaults.conf (spark-submit).
  // You can find the template of this file in $SPARK_CONF/libexec/conf (based on Spark 3.2.1)

  // #2. By setting conf items in spark-submit command line
  // This will replace duplicated confs set in #1

  // #3. By using the API (during SparkSession creation)
  // This will replace duplicated confs set in #1 or #2
  val spark = SparkSession.builder()
    .appName("Learning spark")
    //.config("spark.sql.shuffle.partitions", 5) // Uncomment to apply
    //.config("spark.executor.memory", "2g") // Uncomment to apply
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .master("local")
    .getOrCreate()

  // #4. By using the API (setting them after SparkSession is created)
  // This will replace duplicated confs set in #1, #2 or #3
  spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism) // Setting shuffle partitions to default parallelism

  // CHECKING CONF
  // #1. Using SparkSQL API
  spark.sql("SET -v").select("key", "value").show(15, false)

  // #2. Using Spark UI's environment variables tab

  // Checking if a config item is modifiable: spark.conf.isModifiable("<config_name>")

  // TUNING CONF

  // Normally we would like to have dynamicAllocation that allows as to have an elastic setup
  spark.conf.set("spark.dynamicAllocation.enabled", "true")
  spark.conf.set("spark.dynamicAllocation.minExecutors", "1") // Minimum executors (hot instances)
  spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1m") // Time to pass to spin up new executors if a task is not taken during that period.
  spark.conf.set("spark.dynamicAllocation.maxExecutors", "1") // Maximum executors that this setup can scale out to
  spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "2m") // If an executor is idle for this period and we have executors>minExecutors, Spark will terminate the executor
}
