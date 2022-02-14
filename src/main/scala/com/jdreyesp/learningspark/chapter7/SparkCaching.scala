package com.jdreyesp.learningspark.chapter7

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.storage.StorageLevel

object SparkCaching extends App with SparkSessionInitializer {

  import spark.implicits._

  /** Cache */

  // Process 10M records
  val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")

  // Caching does not happen here. Cached is materialized when an action occurs
  df.cache()
  var init = System.currentTimeMillis()

  // Caching acts here. Depending on the action, Spark can decide what exactly is cached. In case of count(), all partitions will
  // be tried to be saved. If we would have done `take(1)` as an action, it would have stored one partition only
  var res = df.count()
  // You could also cache a table or view derived from a Dataframe using:
  // df.createOrReplaceTempView("dfTable")
  // spark.sql("CACHE TABLE dfTable")
  // spark.sql("SELECT COUNT(*) FROM dfTable").show()

  println(s"First count (cache): $res . Time spent: ${System.currentTimeMillis() - init}ms")

  init = System.currentTimeMillis()
  res = df.count()
  println(s"Second count (cache): $res . Time spent: ${System.currentTimeMillis() - init}ms")

  /** Persist */
  // Depending on the <LEVEL_NAME> option (https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence), it will persist
  // - to memory (serialized in bytes or as object)
  // - to disk (always serialized)
  // - off_heap (sharing same memory as Spark's storage and execution, getting the advantage of not 'suffering' for GC, amongst other benefits)
  // Depending on the <LEVEL_NAME>, there are options to persist to two or more workers in case of
  // data duplicity (for fault tolerance reasons) using `<LEVEL_NAME>_X`

  val df2 = spark.range(1 * 10000000).toDF("id").withColumn("triplet", $"id" * $"id" * $"id")

  df2.persist(StorageLevel.DISK_ONLY)

  init = System.currentTimeMillis()
  res = df2.count()
  println(s"First count (persist): $res . Time spent: ${System.currentTimeMillis() - init}ms")

  init = System.currentTimeMillis()
  res = df2.count()
  println(s"Second count (persist): $res . Time spent: ${System.currentTimeMillis() - init}ms")
}
