package com.jdreyesp.learningspark.chapter7

import org.apache.spark.sql.SparkSession

object SparkTuning extends App {

  /** APPLYING CONF (CONFIGURATION SET IN ORDER OF APPEARANCE IN THIS LIST) **/

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

  /** CHECKING CONF **/

  // #1. Using SparkSQL API
  spark.sql("SET -v").select("key", "value").show(15, false)

  // #2. Using Spark UI's environment variables tab

  // Checking if a config item is modifiable: spark.conf.isModifiable("<config_name>")

  /** RECOMMENDED CONF **/

  // Normally we would like to have dynamicAllocation that allows as to have an elastic setup
  spark.conf.set("spark.dynamicAllocation.enabled", "true")
  spark.conf.set("spark.dynamicAllocation.minExecutors", "1") // Minimum executors (hot instances)
  spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1m") // Time to pass to spin up new executors if a task is not taken during that period.
  spark.conf.set("spark.dynamicAllocation.maxExecutors", "1") // Maximum executors that this setup can scale out to
  spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "2m") // If an executor is idle for this period and we have executors>minExecutors, Spark will terminate the executor

  // If our job has map and shuffle operations
  spark.conf.set("spark.driver.memory", "1gb") // Amount of memory allocated to the Spark driver to receive data from executors
  spark.conf.set("spark.shuffle.file.buffer", "32kb") // Recommended: 1mb. This allows Spark to do more buffering before writing final map results to disk
  spark.conf.set("spark.file.transferTo", "true") // Setting it to false will force Spark to use the file buffer to transfer files before finally writing to disk (decreasing I/O activity)
  spark.conf.set("spark.shuffle.unsafe.file.output.buffer", "32kb") // This controls the ammount of buffering possible when merging files during shuffles. Put it larger (i.e. 1mb) for larger workloads
  spark.conf.set("spark.io.compression.lz4.blockSize", "32kb") // Increase it to 512kb. You can decrease the size of the shuffle file by increasing the compressed size of the block
  spark.conf.set("spark.shuffle.service.index.cache.size", "100m") // Cache entries are limited to the specified memory footprint in byte
  spark.conf.set("spark.shuffle.registration.timeout", "5000ms") // You can increase this to 120000ms
  spark.conf.set("spark.shuffle.registration.maxAttempts", "3") // Increase to 5 if needed

  spark.conf.set("spark.sql.files.maxPartitionBytes", "128m") // Max byte chunk that will be created on disk for a Spark partition. If too small, this could lead to many files, increasing I/O overhead in executors.
  //Alterntive: Use DF repartition() method. example: val ds = spark.read.textFile("../README.md").repartition(16)

  // This number is the number of partitions in spark when shuffling data.
  // If many, this can lead to too much I/O overhead (many small files) that we probably may not need.
  // If using spark-streaming or small datasets you may configure this to the number of cores (or less) in your executors
  spark.conf.set("spark.sql.shuffle.partitions", 200)

  //Transformations like groupBy() or join() (also known as wide transformations) shuffle partitions, therefore consume both network and I/O.
  //The result of the shuffle will spill results into executors' local disks at the location specified in spark.local.directory.
  //Having performant SSD disks for this operation will boost the performance
  spark.conf.set("spark.local.directory", "/<whatever_directory_in_ssd_disk>")


}
