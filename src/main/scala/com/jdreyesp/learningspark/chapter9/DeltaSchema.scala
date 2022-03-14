package com.jdreyesp.learningspark.chapter9

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col

import java.io.File

/**
 * Delta tables schema evolution or prevent data corruption
 */
object DeltaSchema extends App with SparkSessionInitializer {

  import spark.implicits._

  val baseDir = "/tmp/chapter9"

  val sourcePath = getClass.getClassLoader.getResource("chapter9/blogs/blogs.json").getPath
  val sourceStreamingPath1 = getClass.getClassLoader.getResource("chapter9/newBlogs/").getPath
  val checkpointLocation = s"$baseDir/checkpoint"

  val deltaPath = s"$baseDir/loansdelta"

  FileUtils.deleteDirectory(new File(baseDir))

  case class Loan(loanId: Long, fundedAmnt: Float, paidAmnt: Float, addrState: String, closed: Boolean)

  /** Schema evolution */
    val items = Seq(
      Loan(111111L, 1000F, 1000.0F, "TX", false),
      Loan(22222L, 2000F, 0.0F, "CA", true))

  items.toDF().as[Loan].write.format("delta").mode("append").save(deltaPath)

  val loanUpdates2 = spark.createDataFrame(items).as[Loan].withColumn("newAmnt", col("fundedAmnt"))

  //This should throw an exception since schema evolution is not supported
  //loanUpdates2.write.format("delta").mode("append").save(deltaPath)

  // This will insert newAmnts for new rows. Previous rows will have newAmnts = null
  loanUpdates2.write.format("delta").mode("append").option("mergeSchema", "true").save(deltaPath)

  spark.read.format("delta").load(deltaPath).show(false)

}
